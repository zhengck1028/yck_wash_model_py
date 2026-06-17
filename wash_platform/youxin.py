import pandas as pd

from common import config, db
from wash_platform.base import WashPlatformBase


class YouxinWasher(WashPlatformBase):
    platform_name = "youxin"
    source_table = "spider_www_youxin"
    xlat_table = "yck_youxin_modelid_xlat"
    platform_cid_col = "youxin_model_id"
    join_key = "platform_mid"
    incremental_by_update_time = False

    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        df = db.query(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, model_id platform_mid, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '0' state, is_transfer_fee trans_fee,
                       '' annual, '' insure,
                       reg_date regDate,
                       DATE_FORMAT(add_time,'%Y-%m-%d') add_time,
                       DATE_FORMAT(update_time,'%Y-%m-%d') update_time
                FROM {self.source_table}
                WHERE update_time > '{last_upd}'""",
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
        df = self.clean_location(df)
        return df
