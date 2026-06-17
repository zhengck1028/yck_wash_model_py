import pandas as pd

from common import config, db
from wash_platform.base import WashPlatformBase


class Che168Washer(WashPlatformBase):
    platform_name = "che168"
    source_table = "spider_www_168"
    xlat_table = "yck_autohome_modelid_xlat"
    platform_cid_col = "ah_model_id"
    join_key = "platform_mid"
    incremental_by_update_time = False

    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        # quotes 和 mile 原始单位是"分"，需除以 10000
        df = db.query(
            config.DB_YUN,
            f"""SELECT car_id id_data_input, model_id platform_mid,
                       quotes/10000 quotes, address location, location address,
                       transfer, color, mile/10000 mile, state, trans_fee, annual, insure,
                       regDate,
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
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
