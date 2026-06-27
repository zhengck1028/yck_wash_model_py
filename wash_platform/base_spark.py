"""二手车平台清洗——PySpark 基类（模板方法模式）。

与 wash_platform/base.py 业务逻辑等价，但全程用 Spark DataFrame。
各平台 Spark 子类继承本类并实现 fetch_raw_data(newdata, last_upd) -> Spark DataFrame。

读取走 common.spark.read_sql，写回走 write_replace（临时表 MERGE）。
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config, notifier
from common.spark import read_sql, write_replace

# 城市别名修正（各平台共用）
_CITY_ALIASES = {"襄樊": "襄阳", "杨凌": "咸阳", "农垦系统": "哈尔滨"}

OUTPUT_COLS = [
    "car_platform", "id_data_input", "id_che300", "yck_modelid",
    "model_year", "yck_brandid", "yck_seriesid", "is_import", "is_green",
    "brand", "series", "model_name", "color", "liter", "auto",
    "discharge_standard", "car_level", "location", "regDate",
    "quotes", "model_price", "mile", "state", "trans_fee", "transfer",
    "annual", "insure", "add_time", "update_time", "province",
    "car_country", "user_years", "partition_month", "date_add",
]


class WashPlatformSparkBase(ABC):
    platform_name: str = ""
    source_table: str = ""
    xlat_table: str = ""
    platform_cid_col: str = ""
    join_key: str = "platform_cid"
    incremental_by_update_time: bool = True

    def __init__(self) -> None:
        self._yck_che300: DataFrame | None = None
        self._yck_vdb_major_info: DataFrame | None = None
        self._config_distr: DataFrame | None = None
        self._config_distr_all: DataFrame | None = None
        self._config_series_bcountry: DataFrame | None = None

    # ── 查找表（懒加载）──────────────────────────────────────────────

    @property
    def yck_che300(self) -> DataFrame:
        if self._yck_che300 is None:
            self._yck_che300 = read_sql(
                config.DB_VDB, "SELECT yck_modelid, che300_model_id FROM yck_che300_modelid_xlat"
            )
        return self._yck_che300

    @property
    def yck_vdb_major_info(self) -> DataFrame:
        if self._yck_vdb_major_info is None:
            self._yck_vdb_major_info = read_sql(
                config.DB_YUN,
                """SELECT yck_modelid yck_modelid_c300, model_id, Initial, brandid, yck_brandid,
                          brand_name, series_group_name, series_id, yck_seriesid, series_name,
                          model_name, short_name, model_price, model_year, auto, liter,
                          discharge_standard, max_reg_year, min_reg_year, car_level,
                          seat_number, hl_configs, hl_configc, is_green, is_import
                   FROM yckdc_da2_ucar.config_vdatabase_yck_major_info""",
            )
        return self._yck_vdb_major_info

    @property
    def config_distr(self) -> DataFrame:
        if self._config_distr is None:
            self._config_distr = read_sql(
                config.DB_YUN,
                """SELECT DISTINCT regional, b.province, a.key_municipal city
                   FROM config_district a
                   INNER JOIN config_district_regional b ON a.key_province = b.province""",
            )
        return self._config_distr

    @property
    def config_distr_all(self) -> DataFrame:
        if self._config_distr_all is None:
            self._config_distr_all = read_sql(
                config.DB_YUN,
                """SELECT DISTINCT regional, b.province, a.key_municipal city, a.key_county
                   FROM config_district a
                   INNER JOIN config_district_regional b ON a.key_province = b.province""",
            )
        return self._config_distr_all

    @property
    def config_series_bcountry(self) -> DataFrame:
        if self._config_series_bcountry is None:
            self._config_series_bcountry = read_sql(
                config.DB_YUN, "SELECT DISTINCT yck_brandid, car_country FROM config_vdatabase_yck_brand"
            )
        return self._config_series_bcountry

    # ── 步骤 1: 上次更新时间 ──────────────────────────────────────────

    def get_last_update(self) -> str:
        res = read_sql(
            config.DB_YUN,
            f"SELECT max(date_add) last_upd FROM analysis_wide_table_new WHERE car_platform = '{self.platform_name}'",
        )
        rows = res.collect()
        val = rows[0]["last_upd"] if rows else None
        return str(val) if val is not None else "2000-01-01"

    # ── 步骤 2: 获取新增车型映射 ──────────────────────────────────────

    def get_new_model_ids(self, last_upd: str) -> DataFrame:
        if self.incremental_by_update_time:
            sql = (
                f"SELECT yck_modelid, {self.platform_cid_col} {self.join_key} "
                f"FROM {self.xlat_table} WHERE update_time > '{last_upd}'"
            )
        else:
            sql = f"SELECT yck_modelid, {self.platform_cid_col} {self.join_key} FROM {self.xlat_table}"
        newdata = read_sql(config.DB_VDB, sql)
        newdata = newdata.join(self.yck_che300, on="yck_modelid", how="inner")
        # 仅保留 1:1 映射
        cnt = newdata.groupBy(self.join_key).count()
        keep = cnt.filter(F.col("count") == 1).select(self.join_key)
        newdata = newdata.join(keep, on=self.join_key, how="inner")
        return newdata.select(self.join_key, "che300_model_id", "yck_modelid")

    # ── 步骤 3a: 抓取原始数据（子类实现）──────────────────────────────

    @abstractmethod
    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        ...

    # ── 步骤 3b: 城市标准化 ──────────────────────────────────────────

    def clean_location(self, df: DataFrame, use_county_fallback: bool = False) -> DataFrame:
        cities = [r["city"] for r in self.config_distr.select("city").distinct().collect() if r["city"]]
        city_pat = "|".join(re.escape(c) for c in cities)

        loc = F.col("location").cast("string")
        for old, new in _CITY_ALIASES.items():
            loc = F.regexp_replace(loc, re.escape(old), new)
        df = df.withColumn("location", loc)

        def _extract(colname: str) -> F.Column:
            ext = F.regexp_extract(F.col(colname).cast("string"), f"({city_pat})", 1)
            return F.when(ext == "", None).otherwise(ext)

        loc_city = _extract("location")
        # 用 address 补全
        loc_city = F.when(loc_city.isNotNull(), loc_city).otherwise(_extract("address"))

        if use_county_fallback:
            county_rows = self.config_distr_all.select("key_county", "city").distinct().collect()
            counties = [r["key_county"] for r in county_rows if r["key_county"]]
            county_to_city = {r["key_county"]: r["city"] for r in county_rows if r["key_county"]}
            county_pat = "|".join(re.escape(c) for c in counties)
            county_ext = F.regexp_extract(F.col("address").cast("string"), f"({county_pat})", 1)
            mapping = F.create_map([F.lit(x) for kv in county_to_city.items() for x in kv]) if county_to_city else None
            county_city = mapping[county_ext] if mapping is not None else F.lit(None)
            loc_city = F.when(loc_city.isNotNull(), loc_city).otherwise(
                F.when(county_ext != "", county_city).otherwise(F.lit(""))
            )
            loc_city = F.coalesce(loc_city, F.lit(""))

        return df.withColumn("location", loc_city)

    # ── 步骤 3c: 年检/保险日期差值 ────────────────────────────────────

    def clean_date_expiry(self, df: DataFrame, col: str) -> DataFrame:
        """日期列 → '1'(已过期)/'0'(未过期)/''(无数据)。"""
        if col not in df.columns:
            return df.withColumn(col, F.lit(""))
        add_t = F.to_timestamp(F.col("add_time"))
        exp_t = F.to_timestamp(F.col(col))
        diff = F.datediff(add_t, exp_t)
        result = (
            F.when(diff.isNull(), "")
            .when(diff > 0, "1")
            .otherwise("0")
        )
        return df.withColumn(col, result)

    # ── 步骤 3d: 通用清洗（对应 washfun_UserCar_deal）────────────────

    def clean_common(self, df: DataFrame) -> DataFrame:
        today = str(date.today())
        for col in ("annual", "transfer", "insure", "state", "trans_fee"):
            if col in df.columns:
                df = df.withColumn(col, F.coalesce(F.col(col).cast("string"), F.lit("")))
        df = df.withColumn("transfer", F.regexp_replace(F.col("transfer"), r".*数据|NA", ""))
        for col in ("annual", "insure", "state", "trans_fee"):
            if col in df.columns:
                df = df.withColumn(col, F.regexp_replace(F.col(col), "NA", ""))

        # 颜色清洗
        c = F.coalesce(F.col("color").cast("string"), F.lit(""))
        c = F.regexp_replace(c, r"――|-|无数据|null|[0-9]", "")
        c = F.regexp_replace(c, "其他", "其它")
        c = F.regexp_replace(c, "色", "")
        c = F.regexp_replace(c, r"浅|深|象牙|冰川", "")
        df = df.withColumn("color", c)

        df = df.withColumn("date_add", F.lit(today))
        df = df.withColumn("user_years", F.col("user_years").cast("double"))

        # 价格合理性
        ratio = F.col("quotes") / F.col("model_price")
        df = df.filter((ratio > 0.05) & (ratio < 1.5))
        # 新车过滤
        df = df.filter((F.col("user_years") < 1) | (ratio < 1))
        # 注册日期
        reg = F.to_timestamp(F.col("regDate"))
        df = df.filter(reg.isNotNull() & (reg > F.lit("2000-01-01")) & (reg < F.lit(today)))
        return df

    # ── 步骤 3e: 关联车型信息 + 地区 + 国别 ──────────────────────────

    def merge_model_info(self, df: DataFrame, newdata: DataFrame) -> DataFrame:
        df = df.join(newdata, on=self.join_key, how="inner")
        df = df.join(
            self.yck_vdb_major_info.withColumnRenamed("model_id", "che300_model_id"),
            on="che300_model_id", how="inner",
        )
        df = df.join(self.config_distr.withColumnRenamed("city", "location"), on="location", how="inner")
        df = df.join(self.config_series_bcountry, on="yck_brandid", how="inner")
        df = df.withColumn("car_platform", F.lit(self.platform_name))
        df = df.withColumn("mile", F.round(F.col("mile"), 2))
        df = df.withColumn("quotes", F.round(F.col("quotes"), 2))
        # regDate 截断到月
        df = df.withColumn("regDate", F.date_format(F.to_date(F.col("regDate")), "yyyy-MM"))
        df = df.withColumn(
            "user_years",
            F.round(F.datediff(F.to_timestamp(F.col("add_time")), F.to_timestamp(F.col("regDate"))) / 365, 2),
        )
        df = df.withColumn("partition_month", F.date_format(F.to_timestamp(F.col("add_time")), "yyyyMM"))
        return df

    # ── 步骤 4: 写入宽表 ──────────────────────────────────────────────

    def write_output(self, df: DataFrame) -> None:
        # 对齐输出列（缺失补 null）
        cols = [F.col(c) if c in df.columns else F.lit(None).alias(c) for c in OUTPUT_COLS]
        out = df.select(*cols)
        write_replace(config.DB_YUN, "analysis_wide_table_new", out)
        notifier.send_mail("二手车价格平台清洗", f"{self.platform_name} 平台数据清洗更新成功")

    # ── 模板方法 ──────────────────────────────────────────────────────

    def run(self) -> None:
        print(f"[{self.platform_name}] 开始清洗...")
        last_upd = self.get_last_update()
        print(f"[{self.platform_name}] 上次更新: {last_upd}")

        newdata = self.get_new_model_ids(last_upd)
        if len(newdata.head(1)) == 0:
            print(f"[{self.platform_name}] 无新增车型映射，跳过。")
            return

        raw = self.fetch_raw_data(newdata, last_upd)
        if len(raw.head(1)) == 0:
            print(f"[{self.platform_name}] 无新增数据，跳过。")
            return

        merged = self.merge_model_info(raw, newdata)
        cleaned = self.clean_common(merged)
        if len(cleaned.head(1)) == 0:
            print(f"[{self.platform_name}] 清洗后无有效数据，跳过。")
            return

        self.write_output(cleaned)
        print(f"[{self.platform_name}] 完成。")
