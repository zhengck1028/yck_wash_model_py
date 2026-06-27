"""人人车清洗——PySpark 子类。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config
from common.spark import read_sql
from wash_platform.base_spark import WashPlatformSparkBase


class RrcSparkWasher(WashPlatformSparkBase):
    platform_name = "rrc"
    source_table = "spider_www_renren"
    xlat_table = "yck_rrc_carid_xlat"
    platform_cid_col = "rrc_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        ids = [r[self.join_key] for r in newdata.select(self.join_key).collect()]
        if not ids:
            return newdata.limit(0)
        id_str = ", ".join(str(i) for i in ids)
        df = read_sql(
            config.DB_YUN,
            f"""SELECT id id_data_input, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '0' state, is_transfer_fee trans_fee,
                       inspect_date annual, insure_date insure, reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table} WHERE id IN ({id_str})""",
        )
        if len(df.head(1)) == 0:
            return df
        w = Window.partitionBy("id_data_input").orderBy(F.col("update_time").desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        df = df.withColumn("add_time", F.to_date(F.coalesce(F.col("update_time"), F.col("add_time"))))
        df = df.withColumn("id_data_input", F.col("id_data_input").cast("long"))
        df = df.withColumn(self.join_key, F.col("id_data_input"))
        df = self.clean_location(df)
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
