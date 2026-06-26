"""数据库访问层（SQLAlchemy 实现）。

对外暴露 6 个函数，**签名与行为保持稳定**，上层无需感知底层实现：
    query / execute / bulk_update / bulk_upsert / bulk_insert / replace_all

相比裸 PyMySQL，这里用 SQLAlchemy Engine + 连接池：
  - 每个数据库配置（cfg）对应一个 Engine，按连接串缓存复用，避免频繁建连；
  - 连接池自动管理连接生命周期（pool_pre_ping 防失效连接）。

写入语义沿用原实现：
  - bulk_upsert  → REPLACE INTO
  - bulk_insert  → INSERT IGNORE INTO
  - replace_all  → TRUNCATE + INSERT IGNORE
"""
from __future__ import annotations

import threading

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

# ── Engine 缓存（按连接串复用，线程安全）──────────────────────────────
_ENGINES: dict[str, Engine] = {}
_ENGINE_LOCK = threading.Lock()


def _engine_url(cfg: dict) -> URL:
    """用 URL.create 构造连接串，自动转义用户名/密码中的特殊字符。

    密码常含 @ : / 等字符（如 'J1S3g@OBDcky@c2e'），直接拼字符串会
    破坏 URL 结构，必须用 URL.create 让 SQLAlchemy 处理转义。
    """
    return URL.create(
        "mysql+pymysql",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        query={"charset": cfg.get("charset", "utf8")},
    )


def get_engine(cfg: dict) -> Engine:
    """取（或创建）对应该数据库配置的 SQLAlchemy Engine，按连接串缓存复用。"""
    url = _engine_url(cfg)
    key = url.render_as_string(hide_password=False)
    eng = _ENGINES.get(key)
    if eng is None:
        with _ENGINE_LOCK:
            eng = _ENGINES.get(key)
            if eng is None:
                eng = create_engine(
                    url,
                    pool_size=5,
                    max_overflow=10,
                    pool_recycle=3600,      # 1h 回收，避免 MySQL 8h 断连
                    pool_pre_ping=True,     # 取连接前 ping，自动剔除失效连接
                    connect_args={
                        "connect_timeout": 30,
                        "read_timeout": 600,
                        "write_timeout": 600,
                        "ssl_disabled": True,   # 关闭 SSL（沿用原 pymysql 行为）
                    },
                    future=True,
                )
                _ENGINES[key] = eng
    return eng


def get_conn(cfg: dict):
    """兼容旧接口：返回一个 DBAPI 连接（来自连接池）。

    历史上 get_conn 返回裸 pymysql 连接，现改为从 Engine 连接池借出。
    现有上层代码均通过下面的 6 个函数访问 DB，不直接用 get_conn；
    保留此函数仅为向后兼容。
    """
    return get_engine(cfg).raw_connection()


# ── 内部：兼容含字面量 % 的 SQL ────────────────────────────────────────

def _exec(conn, sql: str, params: tuple | None):
    """统一执行：兼容原 pymysql 在 params=None 时不做 % 格式化的行为。

    pymysql 的 cursor.execute 会对 SQL 做 `query % args` 格式化。SQL 里
    若含字面量 %（如 DATE_FORMAT(x,'%Y-%m-%d')）且不传参，会被误当占位
    符报错。原代码靠 cur.execute(sql, None) 跳过格式化；这里在无参时把
    字面量 % 转义为 %% 复刻该行为，有参时原样传（%s 占位符正常工作）。
    """
    if params is None:
        return conn.exec_driver_sql(sql.replace("%", "%%"))
    return conn.exec_driver_sql(sql, tuple(params))


# ── 读 ─────────────────────────────────────────────────────────────────

def query(cfg: dict, sql: str, params: tuple | None = None) -> pd.DataFrame:
    """执行 SELECT，返回 DataFrame（无结果返回空 DataFrame）。"""
    eng = get_engine(cfg)
    with eng.connect() as conn:
        result = _exec(conn, sql, params)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=list(result.keys()))


# ── 写 ─────────────────────────────────────────────────────────────────

def execute(cfg: dict, sql: str, params: tuple | None = None) -> int:
    """执行单条 DML（UPDATE / DELETE / TRUNCATE），返回 rowcount。"""
    eng = get_engine(cfg)
    with eng.begin() as conn:
        result = _exec(conn, sql, params)
        return result.rowcount


def bulk_update(
    cfg: dict,
    sql: str,
    params_list: list[tuple],
    batch_size: int = 2000,
) -> int:
    """批量执行同一条参数化 UPDATE/DELETE，单连接单事务。

    sql 使用 %s 占位符，params_list 为每行的参数元组，返回受影响总行数。
    """
    if not params_list:
        return 0
    eng = get_engine(cfg)
    total = 0
    with eng.begin() as conn:
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i : i + batch_size]
            result = conn.exec_driver_sql(sql, batch)  # executemany
            rc = result.rowcount
            total += rc if rc and rc > 0 else 0
    return total


def _col_names(cols: list) -> str:
    """拼接反引号列名。

    注：原实现需把列名中的字面量 % 转义成 %%（pymysql 的 % 格式化坑）。
    现走 exec_driver_sql + 位置参数，DBAPI 仍会对 SQL 文本做 % 解析，
    故继续转义以兼容 `ee_fast_charge(%)` 这类含 % 的列名。
    """
    return ", ".join(f"`{str(c).replace('%', '%%')}`" for c in cols)


def bulk_upsert(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """REPLACE INTO（按主键/唯一键覆盖）。"""
    if df.empty:
        return
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"REPLACE INTO `{table}` ({_col_names(cols)}) VALUES ({placeholders})"
    _batch_execute(cfg, sql, df, batch_size)


def bulk_insert(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """INSERT IGNORE INTO（冲突则跳过）。"""
    if df.empty:
        return
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT IGNORE INTO `{table}` ({_col_names(cols)}) VALUES ({placeholders})"
    _batch_execute(cfg, sql, df, batch_size)


def replace_all(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """TRUNCATE + INSERT IGNORE（整表替换）。"""
    eng = get_engine(cfg)
    with eng.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE `{table}`")
    bulk_insert(cfg, table, df, batch_size)


def _batch_execute(cfg: dict, sql: str, df: pd.DataFrame, batch_size: int) -> None:
    rows = _df_to_rows(df)
    eng = get_engine(cfg)
    with eng.begin() as conn:
        for i in range(0, len(rows), batch_size):
            conn.exec_driver_sql(sql, rows[i : i + batch_size])  # executemany


def _df_to_rows(df: pd.DataFrame) -> list[tuple]:
    """将 DataFrame 转为 list[tuple]，None/NaN 统一为 None（MySQL 写入为 NULL）。"""
    return [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
