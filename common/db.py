from __future__ import annotations

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor
from typing import Any


def get_conn(cfg: dict) -> pymysql.Connection:
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8"),
        cursorclass=DictCursor,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=600,
        ssl_disabled=True,
    )


def query(cfg: dict, sql: str, params: tuple | None = None) -> pd.DataFrame:
    """对应 R 的 fun_mysqlload_query（SELECT）"""
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        conn.close()


def execute(cfg: dict, sql: str, params: tuple | None = None) -> int:
    """执行单条 DML（UPDATE / DELETE / TRUNCATE），返回 rowcount"""
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def bulk_update(
    cfg: dict,
    sql: str,
    params_list: list[tuple],
    batch_size: int = 2000,
) -> int:
    """批量执行同一条参数化 UPDATE/DELETE，单连接单提交。

    sql 使用 %s 占位符，params_list 为每行的参数元组。
    返回受影响总行数。
    """
    if not params_list:
        return 0
    conn = get_conn(cfg)
    total = 0
    try:
        with conn.cursor() as cur:
            for i in range(0, len(params_list), batch_size):
                total += cur.executemany(sql, params_list[i : i + batch_size])
        conn.commit()
    finally:
        conn.close()
    return total


def _col_names(cols: list) -> str:
    """拼接反引号列名，并把列名中的字面量 % 转义成 %%。

    pymysql 的 executemany 会对 SQL 做 % 格式化，列名里裸 % （如
    `ee_fast_charge(%)`）会被误当占位符而报 TypeError。
    """
    return ", ".join(f"`{str(c).replace('%', '%%')}`" for c in cols)


def bulk_upsert(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """对应 R 的 fun_mysqlload_add_upd（REPLACE INTO）"""
    if df.empty:
        return
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"REPLACE INTO `{table}` ({_col_names(cols)}) VALUES ({placeholders})"
    _batch_execute(cfg, sql, df, batch_size)


def bulk_insert(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """对应 R 的 fun_mysqlload_add（INSERT IGNORE INTO）"""
    if df.empty:
        return
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT IGNORE INTO `{table}` ({_col_names(cols)}) VALUES ({placeholders})"
    _batch_execute(cfg, sql, df, batch_size)


def replace_all(cfg: dict, table: str, df: pd.DataFrame, batch_size: int = 2000) -> None:
    """对应 R 的 fun_mysqlload_all（TRUNCATE + INSERT）"""
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE `{table}`")
        conn.commit()
    finally:
        conn.close()
    bulk_insert(cfg, table, df, batch_size)


def _batch_execute(cfg: dict, sql: str, df: pd.DataFrame, batch_size: int) -> None:
    rows = _df_to_rows(df)
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                cur.executemany(sql, rows[i : i + batch_size])
        conn.commit()
    finally:
        conn.close()


def _df_to_rows(df: pd.DataFrame) -> list[tuple]:
    """将 DataFrame 转为 list[tuple]，None 保持为 None（MySQL 写入为 NULL）"""
    return [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
