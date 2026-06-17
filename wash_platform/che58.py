import pandas as pd

from common import config, db
from wash_platform.base import WashPlatformBase


class Che58Washer(WashPlatformBase):
    platform_name = "che58"
    source_table = "spider_www_58"
    xlat_table = "yck_che58_carid_xlat"
    platform_cid_col = "che58_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        ids = newdata[self.join_key].tolist()
        id_str = ", ".join(str(i) for i in ids)

        # 去重：同一 car_id 有多条爬虫记录时，保留本次新增 id，删除旧记录
        car_ids_df = db.query(
            config.DB_YUN,
            f"SELECT DISTINCT car_id FROM {self.source_table} WHERE id IN ({id_str})",
        )
        if not car_ids_df.empty:
            car_id_str = ", ".join(str(i) for i in car_ids_df["car_id"].tolist())
            dup_df = db.query(
                config.DB_YUN,
                f"""SELECT car_id, COUNT(*) cnt FROM {self.source_table}
                    WHERE car_id IN ({car_id_str}) GROUP BY car_id HAVING cnt > 1""",
            )
            if not dup_df.empty:
                dup_car_id_str = ", ".join(str(i) for i in dup_df["car_id"].tolist())
                all_dup_ids_df = db.query(
                    config.DB_YUN,
                    f"SELECT id FROM {self.source_table} WHERE car_id IN ({dup_car_id_str})",
                )
                old_ids = [i for i in all_dup_ids_df["id"].tolist() if i not in ids]
                if old_ids:
                    old_id_str = ", ".join(str(i) for i in old_ids)
                    db.execute(
                        config.DB_YUN,
                        f"DELETE FROM analysis_wide_table_new WHERE car_platform='{self.platform_name}' AND id_data_input IN ({old_id_str})",
                    )
                    db.execute(
                        config.DB_YUN,
                        f"DELETE FROM {self.source_table} WHERE id IN ({old_id_str})",
                    )

        df = db.query(
            config.DB_YUN,
            f"""SELECT id id_data_input, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '1' state, is_transfer_fee trans_fee,
                       inspect_date annual, insure_date insure,
                       reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table}
                WHERE id IN ({id_str})""",
        )
        if df.empty:
            return df
        df = (
            df.sort_values("update_time", ascending=False)
            .drop_duplicates(subset=["id_data_input"])
        )
        df["add_time"] = df["update_time"].fillna(df["add_time"])
        df["add_time"] = pd.to_datetime(df["add_time"], errors="coerce")
        df["id_data_input"] = df["id_data_input"].astype(int)
        df[self.join_key] = df["id_data_input"]

        # 过户费标准化
        df["trans_fee"] = df["trans_fee"].astype(str)
        df.loc[df["trans_fee"].str.contains("不含", na=False), "trans_fee"] = "0"
        df.loc[df["trans_fee"].str.contains("含", na=False) & ~df["trans_fee"].str.contains("不含", na=False), "trans_fee"] = "1"
        df.loc[df["trans_fee"].isna(), "trans_fee"] = ""

        df = self.clean_location(df)
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
