import pandas as pd

from common import config, db
from wash_platform.base import WashPlatformBase


class RrcWasher(WashPlatformBase):
    platform_name = "rrc"
    source_table = "spider_www_renren"
    xlat_table = "yck_rrc_carid_xlat"
    platform_cid_col = "rrc_car_id"
    join_key = "platform_cid"
    incremental_by_update_time = True

    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        ids = newdata[self.join_key].tolist()
        id_str = ", ".join(str(i) for i in ids)
        df = db.query(
            config.DB_YUN,
            f"""SELECT id id_data_input, price quotes, location, place address,
                       transfer_number transfer, body_color color, mile, '0' state, is_transfer_fee trans_fee,
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
        df = self.clean_location(df)
        df = self.clean_date_expiry(df, "annual")
        df = self.clean_date_expiry(df, "insure")
        return df
