"""数据库与告警配置。

敏感信息（密码）一律从环境变量 / .env 读取，**不在代码中硬编码**。
本地开发：复制 .env.example 为 .env 并填入真实值。
生产部署：用 AWS SSM Parameter Store 或容器环境变量注入。

非敏感的 host/port/库名保留默认值，方便快速本地运行。
"""
import os

try:
    from dotenv import load_dotenv
    # 加载项目根目录的 .env（仅本地开发；生产用 SSM/环境变量注入）
    load_dotenv()
except ImportError:
    pass


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# ── ODS 层（yck_ods，ETL 中间层；已迁移到 NAS MySQL）────────────────────
DB_ODS = {
    "host": _get("DB_ODS_HOST", "localhost"),
    "port": int(_get("DB_ODS_PORT", "3306")),
    "user": _get("DB_ODS_USER", "root"),
    "password": _get("DB_ODS_PASS"),
    "database": _get("DB_ODS_NAME", "yck_ods"),
    "charset": "utf8",
}

# ── 爬虫库（yck-data-center，数据源；已迁移到 NAS MySQL）────────────────
DB_LOCAL = {
    "host": _get("DB_LOCAL_HOST", "localhost"),
    "port": int(_get("DB_LOCAL_PORT", "3306")),
    "user": _get("DB_LOCAL_USER", "root"),
    "password": _get("DB_LOCAL_PASS"),
    "database": _get("DB_LOCAL_NAME", "yck-data-center"),
    "charset": "utf8",
}

# ── 阿里云数据中台（已废弃，仅历史模块引用）────────────────────────────
DB_YUN = {
    "host": _get("DB_YUN_HOST", "localhost"),
    "port": int(_get("DB_YUN_PORT", "3306")),
    "user": _get("DB_YUN_USER", "root"),
    "password": _get("DB_YUN_PASS"),
    "database": _get("DB_YUN_NAME", ""),
    "charset": "utf8",
}

# ── IT 云端生产库（最终写入目标）────────────────────────────────────────
DB_IT = {
    "host": _get("DB_IT_HOST", "localhost"),
    "port": int(_get("DB_IT_PORT", "3306")),
    "user": _get("DB_IT_USER", "root"),
    "password": _get("DB_IT_PASS"),
    "database": _get("DB_IT_NAME", ""),
    "charset": "utf8",
}

# ── IT 云端测试库 ──────────────────────────────────────────────────────
DB_IT_TEST = {
    "host": _get("DB_IT_TEST_HOST", "localhost"),
    "port": int(_get("DB_IT_TEST_PORT", "3306")),
    "user": _get("DB_IT_TEST_USER", "root"),
    "password": _get("DB_IT_TEST_PASS"),
    "database": _get("DB_IT_TEST_NAME", ""),
    "charset": "utf8",
}

# ── 通用车型库（generalcardb）──────────────────────────────────────────
DB_VDB = {
    "host": _get("DB_VDB_HOST", "localhost"),
    "port": int(_get("DB_VDB_PORT", "3306")),
    "user": _get("DB_VDB_USER", "root"),
    "password": _get("DB_VDB_PASS"),
    "database": _get("DB_VDB_NAME", ""),
    "charset": "utf8",
}

# ── 邮件告警 ────────────────────────────────────────────────────────────
SMTP = {
    "host": _get("SMTP_HOST", "smtp.163.com"),
    "port": int(_get("SMTP_PORT", "465")),
    "user": _get("SMTP_USER", ""),
    "password": _get("SMTP_PASS"),
    "from": _get("SMTP_FROM", ""),
    "to": [x for x in _get("SMTP_TO", "").split(",") if x],
}
