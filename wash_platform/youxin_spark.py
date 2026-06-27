"""优信清洗——PySpark 子类。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config
from common.spark import read_sql
from wash_platform.base_spark import WashPlatformSparkBase


class YouxinSparkWasher(WashPlatformSparkBase):
    platform_name = "youxin"
    source_table = "spider_www_youxin"
    xlat_table = "yck_youxin_modelid_xlat"
    platform_cid_col = "youxin_model_id"
    join_key = "platform_mid"
    incremental_by_update_time = False

    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        df = read_sql(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, model_id platform_mid, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '0' state, is_transfer_fee trans_fee,
                       '' annual, '' insure, reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table} WHERE update_time > '{last_upd}'""",
        )
        if len(df.head(1)) == 0:
            return df
        w = Window.partitionBy("id_data_input").orderBy(F.col("update_time").desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        df = df.withColumn("add_time", F.to_date(F.coalesce(F.col("update_time"), F.col("add_time"))))
        df = df.withColumn("id_data_input", F.col("id_data_input").cast("long"))
        df = self.clean_location(df)
        return df
