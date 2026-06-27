"""58汽车清洗——PySpark 子类。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config, db
from common.spark import read_sql
from wash_platform.base_spark import WashPlatformSparkBase


class Che58SparkWasher(WashPlatformSparkBase):
    platform_name = "che58"
    source_table = "spider_www_58"
    xlat_table = "yck_che58_carid_xlat"
    platform_cid_col = "che58_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: DataFrame, last_upd: str) -> DataFrame:
        ids = [r[self.join_key] for r in newdata.select(self.join_key).collect()]
        if not ids:
            return newdata.limit(0)
        id_str = ", ".join(str(i) for i in ids)

        # 去重：同一 car_id 多条爬虫记录，删除旧记录（点删除走 db.execute）
        car_ids = [
            r["car_id"]
            for r in read_sql(config.DB_YUN, f"SELECT DISTINCT car_id FROM {self.source_table} WHERE id IN ({id_str})").collect()
        ]
        if car_ids:
            car_id_str = ", ".join(str(i) for i in car_ids)
            dup_ids = [
                r["car_id"]
                for r in read_sql(
                    config.DB_YUN,
                    f"SELECT car_id, COUNT(*) cnt FROM {self.source_table} WHERE car_id IN ({car_id_str}) GROUP BY car_id HAVING cnt > 1",
                ).collect()
            ]
            if dup_ids:
                dup_str = ", ".join(str(i) for i in dup_ids)
                all_dup = [
                    r["id"]
                    for r in read_sql(config.DB_YUN, f"SELECT id FROM {self.source_table} WHERE car_id IN ({dup_str})").collect()
                ]
                old_ids = [i for i in all_dup if i not in ids]
                if old_ids:
                    old_str = ", ".join(str(i) for i in old_ids)
                    db.execute(
                        config.DB_YUN,
                        f"DELETE FROM analysis_wide_table_new WHERE car_platform='{self.platform_name}' AND id_data_input IN ({old_str})",
                    )
                    db.execute(config.DB_YUN, f"DELETE FROM {self.source_table} WHERE id IN ({old_str})")

        df = read_sql(
            config.DB_YUN,
            f"""SELECT id id_data_input, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '1' state, is_transfer_fee trans_fee,
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
        # 过户费标准化
        tf = F.col("trans_fee").cast("string")
        df = df.withColumn(
            "trans_fee",
            F.when(tf.contains("不含"), "0")
            .when(tf.contains("含"), "1")
            .when(tf.isNull(), "")
            .otherwise(tf),
        )
        df = self.clean_location(df)
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
