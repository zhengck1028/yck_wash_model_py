"""瓜子二手车清洗——PySpark 子类。"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config
from common.spark import read_sql
from wash_platform.base_spark import WashPlatformSparkBase


class GuaziSparkWasher(WashPlatformSparkBase):
    platform_name = "guazi"
    source_table = "spider_www_guazi"
    xlat_table = "yck_guazi_modelid_xlat"
    platform_cid_col = "guazi_model_id"
    join_key = "platform_mid"
    incremental_by_update_time = False

    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        df = read_sql(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, model_id platform_mid, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '0' state, is_transfer_fee trans_fee,
                       inspect_date annual, insure_date insure,
                       reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table}
                WHERE update_time > '{last_upd}'""",
        )
        if len(df.head(1)) == 0:
            return df
        # 同一 id_data_input 取 update_time 最新
        from pyspark.sql import Window
        w = Window.partitionBy("id_data_input").orderBy(F.col("update_time").desc_nulls_last())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
        df = df.withColumn("add_time", F.coalesce(F.col("update_time"), F.col("add_time")))
        df = df.withColumn("add_time", F.to_date(F.col("add_time")))
        df = df.withColumn("id_data_input", F.col("id_data_input").cast("long"))
        # 括号内城市噪声清洗
        df = df.withColumn("location", F.regexp_replace(F.col("location"), r"[\(（][^\)）]*[\)）]", ""))
        df = self.clean_location(df, use_county_fallback=True)
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
