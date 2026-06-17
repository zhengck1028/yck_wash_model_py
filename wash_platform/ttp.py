import pandas as pd

from common import config, db
from wash_platform.base import WashPlatformBase


class TtpWasher(WashPlatformBase):
    platform_name = "ttp"
    source_table = "spider_www_ttp"
    xlat_table = "yck_ttp_carid_xlat"
    platform_cid_col = "ttp_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        ids = newdata[self.join_key].tolist()
        id_str = ", ".join(str(i) for i in ids)
        df = db.query(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, deal_price quotes, deal_date, location, place address,
                       transfer_number transfer, body_color color, mile, '1' state, '' trans_fee,
                       '' annual, '' insure,
                       reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table}
                WHERE car_id IN ({id_str})""",
        )
        if df.empty:
            return df
        # 保留每条 car_id 最新的一条
        df = (
            df.sort_values("update_time", ascending=False)
            .drop_duplicates(subset=["id_data_input"])
        )
        # 用 deal_date 填补 add_time
        df["add_time"] = df["add_time"].fillna(df["update_time"])
        df["deal_date"] = df["deal_date"].fillna(df["add_time"])
        df["add_time"] = pd.to_datetime(df["deal_date"], errors="coerce")
        df["id_data_input"] = df["id_data_input"].astype(int)
        df[self.join_key] = df["id_data_input"]
        df = self.clean_location(df)
        return df
