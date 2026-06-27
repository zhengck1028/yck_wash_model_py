"""天天拍车清洗——PySpark 子类。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config
from common.spark import read_sql
from wash_platform.base_spark import WashPlatformSparkBase


class TtpSparkWasher(WashPlatformSparkBase):
    platform_name = "ttp"
    source_table = "spider_www_ttp"
    xlat_table = "yck_ttp_carid_xlat"
    platform_cid_col = "ttp_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        ids = [r[self.join_key] for r in newdata.select(self.join_key).collect()]
        if not ids:
            return newdata.limit(0)
        id_str = ", ".join(str(i) for i in ids)
        df = read_sql(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, deal_price quotes, deal_date, location, place address,
                       transfer_number transfer, body_color color, mile, '1' state, '' trans_fee,
                       '' annual, '' insure, reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table} WHERE car_id IN ({id_str})""",
        )
        if len(df.head(1)) == 0:
            return df
        w = Window.partitionBy("id_data_input").orderBy(F.col("update_time").desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        # 用 deal_date 填补 add_time（对应 pandas：add_time<-update_time, deal_date<-add_time, add_time<-deal_date）
        df = df.withColumn("add_time", F.coalesce(F.col("add_time"), F.col("update_time")))
        df = df.withColumn("deal_date", F.coalesce(F.col("deal_date"), F.col("add_time")))
        df = df.withColumn("add_time", F.to_date(F.col("deal_date")))
        df = df.withColumn("id_data_input", F.col("id_data_input").cast("long"))
        df = df.withColumn(self.join_key, F.col("id_data_input"))
        df = self.clean_location(df)
        return df
