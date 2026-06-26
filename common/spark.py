"""Spark 运行环境与 MySQL 读写封装。

提供：
  - get_spark(): SparkSession 工厂，默认 local[*] 单机模式，可通过环境变量
    SPARK_MASTER 切换到集群（如 spark://host:7077 或 yarn）。
  - read_table() / read_sql(): 从 MySQL 读为 Spark DataFrame（JDBC）。
  - write_replace() / write_insert(): 写回 MySQL，保留 REPLACE INTO /
    INSERT IGNORE 语义（Spark JDBC 原生不支持，故按分区收集后复用
    common.db 的批量写实现）。

环境要求：
  - Java 17+（PySpark 4.x），JAVA_HOME 指向其安装目录
  - MySQL JDBC 驱动 jar：通过环境变量 MYSQL_JDBC_JAR 指定，或放入 Spark
    的 jars 目录；spark.jars.packages 也可在线拉取 mysql-connector-j
"""
from __future__ import annotations

import os
from functools import lru_cache

from pyspark.sql import DataFrame, SparkSession

from common import config, db


# ── SparkSession ───────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_spark(app_name: str = "yck_wash_model") -> SparkSession:
    """取（或创建）SparkSession。

    默认 local[*]；设 SPARK_MASTER 环境变量可切换到集群。
    内存按 NAS 小资源场景给保守默认，可用环境变量覆盖。
    """
    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "2g"))
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        # 小数据场景：减少默认 200 分区的 shuffle 开销
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    )
    # MySQL JDBC 驱动：本地 jar 或在线包二选一
    jar = os.environ.get("MYSQL_JDBC_JAR")
    if jar:
        builder = builder.config("spark.jars", jar)
    else:
        builder = builder.config(
            "spark.jars.packages",
            os.environ.get("MYSQL_JDBC_PACKAGE", "com.mysql:mysql-connector-j:8.3.0"),
        )
    return builder.getOrCreate()


# ── JDBC 连接属性 ──────────────────────────────────────────────────────

def _jdbc_url(cfg: dict) -> str:
    return (
        f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}"
        f"?useSSL=false&characterEncoding={cfg.get('charset', 'utf8')}"
        f"&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai"
    )


def _jdbc_props(cfg: dict) -> dict:
    return {
        "user": cfg["user"],
        "password": cfg["password"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }


# ── 读 ─────────────────────────────────────────────────────────────────

def read_table(cfg: dict, table: str) -> DataFrame:
    """读整张表为 Spark DataFrame。"""
    return get_spark().read.jdbc(
        url=_jdbc_url(cfg), table=f"`{table}`", properties=_jdbc_props(cfg)
    )


def read_sql(cfg: dict, sql: str) -> DataFrame:
    """以子查询方式下推 SQL 读取（dbtable=(sql) AS t）。"""
    subquery = f"({sql}) AS _sub"
    return get_spark().read.jdbc(
        url=_jdbc_url(cfg), table=subquery, properties=_jdbc_props(cfg)
    )


# ── 写（纯 Spark：临时表 + SQL MERGE，保留 MySQL 特有语义）──────────────
#
# Spark JDBC 原生不支持 REPLACE INTO / INSERT IGNORE。这里走：
#   1) Spark 用 JDBC append 把结果写到一张与目标同结构的临时表；
#   2) 用一条 INSERT ... SELECT ... ON DUPLICATE KEY UPDATE（=REPLACE 语义）
#      或 INSERT IGNORE ... SELECT（=INSERT IGNORE 语义）从临时表合并入目标；
#   3) 删临时表。
# 合并 SQL 走 common.db（SQLAlchemy）执行，不经过 pandas。

import uuid


def _tmp_table_name(table: str) -> str:
    return f"_spark_tmp_{table}_{uuid.uuid4().hex[:8]}"


def _stage_to_tmp(cfg: dict, table: str, sdf: DataFrame) -> tuple[str, list[str]]:
    """把 sdf 写入与目标表同结构的临时表，返回 (临时表名, 列名列表)。"""
    tmp = _tmp_table_name(table)
    # 用目标表结构建临时表（含主键，供 ON DUPLICATE KEY 生效）
    db.execute(cfg, f"CREATE TABLE `{tmp}` LIKE `{table}`")
    cols = sdf.columns
    # Spark JDBC append 写临时表
    sdf.write.jdbc(
        url=_jdbc_url(cfg), table=f"`{tmp}`", mode="append", properties=_jdbc_props(cfg)
    )
    return tmp, cols


def _merge_columns(cols: list[str]) -> str:
    return ", ".join(f"`{c}`" for c in cols)


def write_replace(cfg: dict, table: str, sdf: DataFrame) -> None:
    """REPLACE 语义：临时表 + INSERT ... ON DUPLICATE KEY UPDATE 合并。"""
    if sdf.rdd.isEmpty():
        return
    tmp, cols = _stage_to_tmp(cfg, table, sdf)
    try:
        col_list = _merge_columns(cols)
        updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in cols)
        db.execute(
            cfg,
            f"INSERT INTO `{table}` ({col_list}) SELECT {col_list} FROM `{tmp}` "
            f"ON DUPLICATE KEY UPDATE {updates}",
        )
    finally:
        db.execute(cfg, f"DROP TABLE IF EXISTS `{tmp}`")


def write_insert(cfg: dict, table: str, sdf: DataFrame) -> None:
    """INSERT IGNORE 语义：临时表 + INSERT IGNORE ... SELECT 合并。"""
    if sdf.rdd.isEmpty():
        return
    tmp, cols = _stage_to_tmp(cfg, table, sdf)
    try:
        col_list = _merge_columns(cols)
        db.execute(
            cfg,
            f"INSERT IGNORE INTO `{table}` ({col_list}) SELECT {col_list} FROM `{tmp}`",
        )
    finally:
        db.execute(cfg, f"DROP TABLE IF EXISTS `{tmp}`")
