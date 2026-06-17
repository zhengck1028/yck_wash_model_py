from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import date, datetime

import pandas as pd

from common import config, db, notifier


# 城市别名修正（各平台共用）
_CITY_ALIASES = {
    "襄樊": "襄阳",
    "杨凌": "咸阳",
    "农垦系统": "哈尔滨",
}

OUTPUT_COLS = [
    "car_platform", "id_data_input", "id_che300", "yck_modelid",
    "model_year", "yck_brandid", "yck_seriesid", "is_import", "is_green",
    "brand", "series", "model_name", "color", "liter", "auto",
    "discharge_standard", "car_level", "location", "regDate",
    "quotes", "model_price", "mile", "state", "trans_fee", "transfer",
    "annual", "insure", "add_time", "update_time", "province",
    "car_country", "user_years", "partition_month", "date_add",
]


class WashPlatformBase(ABC):
    platform_name: str = ""
    source_table: str = ""
    xlat_table: str = ""
    platform_cid_col: str = ""   # 平台 ID 列名（xlat 表中）
    join_key: str = "platform_cid"  # 与 raw data 关联的列名

    # 是否按 update_time 增量（True）还是全量取映射（False）
    incremental_by_update_time: bool = True

    def __init__(self) -> None:
        self._yck_che300: pd.DataFrame | None = None
        self._yck_vdb_major_info: pd.DataFrame | None = None
        self._config_distr: pd.DataFrame | None = None
        self._config_distr_all: pd.DataFrame | None = None
        self._config_series_bcountry: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # 全局查找表（懒加载，函数内共用）
    # ------------------------------------------------------------------

    @property
    def yck_che300(self) -> pd.DataFrame:
        if self._yck_che300 is None:
            self._yck_che300 = db.query(
                config.DB_VDB,
                "SELECT yck_modelid, che300_model_id FROM yck_che300_modelid_xlat",
            )
        return self._yck_che300

    @property
    def yck_vdb_major_info(self) -> pd.DataFrame:
        if self._yck_vdb_major_info is None:
            self._yck_vdb_major_info = db.query(
                config.DB_YUN,
                """SELECT yck_modelid yck_modelid_c300, model_id, Initial, brandid, yck_brandid,
                          brand_name, series_group_name, series_id, yck_seriesid, series_name,
                          model_name, short_name, model_price, model_year, auto, liter,
                          discharge_standard, max_reg_year, min_reg_year, car_level,
                          seat_number, hl_configs, hl_configc, is_green, is_import
                   FROM yckdc_da2_ucar.config_vdatabase_yck_major_info""",
            )
        return self._yck_vdb_major_info

    @property
    def config_distr(self) -> pd.DataFrame:
        if self._config_distr is None:
            self._config_distr = db.query(
                config.DB_YUN,
                """SELECT DISTINCT regional, b.province, a.key_municipal city
                   FROM config_district a
                   INNER JOIN config_district_regional b ON a.key_province = b.province""",
            )
        return self._config_distr

    @property
    def config_distr_all(self) -> pd.DataFrame:
        if self._config_distr_all is None:
            self._config_distr_all = db.query(
                config.DB_YUN,
                """SELECT DISTINCT regional, b.province, a.key_municipal city, a.key_county
                   FROM config_district a
                   INNER JOIN config_district_regional b ON a.key_province = b.province""",
            )
        return self._config_distr_all

    @property
    def config_series_bcountry(self) -> pd.DataFrame:
        if self._config_series_bcountry is None:
            self._config_series_bcountry = db.query(
                config.DB_YUN,
                "SELECT DISTINCT yck_brandid, car_country FROM config_vdatabase_yck_brand",
            )
        return self._config_series_bcountry

    # ------------------------------------------------------------------
    # 步骤 1: 上次更新时间
    # ------------------------------------------------------------------

    def get_last_update(self) -> str:
        result = db.query(
            config.DB_YUN,
            f"SELECT max(date_add) last_upd FROM analysis_wide_table_new WHERE car_platform = '{self.platform_name}'",
        )
        val = result["last_upd"].iloc[0] if not result.empty else None
        return str(val) if val is not None else "2000-01-01"

    # ------------------------------------------------------------------
    # 步骤 2: 获取新增车型映射
    # ------------------------------------------------------------------

    def get_new_model_ids(self, last_upd: str) -> pd.DataFrame:
        if self.incremental_by_update_time:
            sql = (
                f"SELECT yck_modelid, {self.platform_cid_col} {self.join_key} "
                f"FROM {self.xlat_table} WHERE update_time > '{last_upd}'"
            )
        else:
            sql = (
                f"SELECT yck_modelid, {self.platform_cid_col} {self.join_key} "
                f"FROM {self.xlat_table}"
            )
        newdata = db.query(config.DB_VDB, sql)
        if newdata.empty:
            return newdata
        newdata = newdata.merge(self.yck_che300, on="yck_modelid", how="inner")
        # 仅保留 1:1 映射
        cnt = newdata.groupby(self.join_key).size().reset_index(name="cnt")
        newdata = newdata.merge(cnt, on=self.join_key).query("cnt == 1").drop(columns="cnt")
        return newdata[[self.join_key, "che300_model_id", "yck_modelid"]]

    # ------------------------------------------------------------------
    # 步骤 3a: 抓取原始数据（子类实现）
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_raw_data(self, newdata: pd.DataFrame, last_upd: str) -> pd.DataFrame:
        ...

    # ------------------------------------------------------------------
    # 步骤 3b: 城市标准化
    # ------------------------------------------------------------------

    def clean_location(self, df: pd.DataFrame, use_county_fallback: bool = False) -> pd.DataFrame:
        cities = self.config_distr["city"].tolist()
        city_pattern = "|".join(re.escape(c) for c in cities)

        for old, new in _CITY_ALIASES.items():
            df["location"] = df["location"].str.replace(old, new, regex=False)

        def extract_city(s: str | None) -> str | None:
            if pd.isna(s):
                return None
            m = re.search(city_pattern, str(s))
            return m.group(0) if m else None

        loc = df["location"].apply(extract_city)
        # 用 address 补全
        missing = loc.isna()
        loc[missing] = df.loc[missing, "address"].apply(extract_city)

        if use_county_fallback:
            # 用县级匹配再补全
            counties = self.config_distr_all["key_county"].tolist()
            county_pattern = "|".join(re.escape(c) for c in counties)
            county_map = self.config_distr_all.drop_duplicates("key_county").set_index("key_county")["city"]

            still_missing = loc.isna()
            def county_to_city(s):
                if pd.isna(s):
                    return None
                m = re.search(county_pattern, str(s))
                if m:
                    return county_map.get(m.group(0))
                return None

            loc[still_missing] = df.loc[still_missing, "address"].apply(county_to_city)
            loc = loc.fillna("")

        df["location"] = loc
        return df

    # ------------------------------------------------------------------
    # 步骤 3c: 年检/保险日期差值
    # ------------------------------------------------------------------

    def clean_date_expiry(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """将日期列转为 '1'（已过期）/ '0'（未过期）/ ''（无数据）"""
        if col not in df.columns:
            df[col] = ""
            return df
        add_time = pd.to_datetime(df["add_time"], errors="coerce")
        exp_date = pd.to_datetime(df[col], errors="coerce")
        diff = (add_time - exp_date).dt.days
        result = diff.apply(lambda d: "1" if pd.notna(d) and d > 0 else ("0" if pd.notna(d) else ""))
        df[col] = result
        return df

    # ------------------------------------------------------------------
    # 步骤 3d: 通用清洗（对应 R washfun_UserCar_deal）
    # ------------------------------------------------------------------

    def clean_common(self, df: pd.DataFrame) -> pd.DataFrame:
        today = str(date.today())

        for col in ["annual", "transfer", "insure", "state", "trans_fee"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        df["transfer"] = df["transfer"].str.replace(r".*数据|NA", "", regex=True)
        for col in ["annual", "insure", "state", "trans_fee"]:
            if col in df.columns:
                df[col] = df[col].str.replace("NA", "", regex=False)

        # 颜色清洗
        df["color"] = df["color"].fillna("").astype(str)
        df["color"] = df["color"].str.replace(r"――|-|无数据|null|[0-9]", "", regex=True)
        df["color"] = df["color"].str.replace("其他", "其它", regex=False)
        df["color"] = df["color"].str.replace("色", "", regex=False)
        df["color"] = df["color"].str.replace(r"浅|深|象牙|冰川", "", regex=True)

        df["date_add"] = today
        df["user_years"] = pd.to_numeric(df["user_years"], errors="coerce")

        # 价格合理性
        ratio = df["quotes"] / df["model_price"]
        df = df[(ratio > 0.05) & (ratio < 1.5)]
        # 新车过滤
        df = df[(df["user_years"] < 1) | (ratio < 1)]
        # 注册日期
        reg = pd.to_datetime(df["regDate"], errors="coerce")
        df = df[reg.notna() & (reg > pd.Timestamp("2000-01-01")) & (reg < pd.Timestamp(today))]

        return df

    # ------------------------------------------------------------------
    # 步骤 3e: 关联车型信息 + 地区 + 国别
    # ------------------------------------------------------------------

    def merge_model_info(self, df: pd.DataFrame, newdata: pd.DataFrame) -> pd.DataFrame:
        # 关联映射
        df = df.merge(newdata, on=self.join_key, how="inner")
        # 关联 YCK 车型库
        df = df.merge(
            self.yck_vdb_major_info.rename(columns={"model_id": "che300_model_id"}),
            on="che300_model_id",
            how="inner",
        )
        # 关联城市-省份
        df = df.merge(self.config_distr.rename(columns={"city": "location"}), on="location", how="inner")
        # 关联品牌国别
        df = df.merge(self.config_series_bcountry, on="yck_brandid", how="inner")
        df["car_platform"] = self.platform_name
        df["mile"] = df["mile"].round(2)
        df["quotes"] = df["quotes"].round(2)
        # regDate 截断到月
        df["regDate"] = pd.to_datetime(df["regDate"], errors="coerce").dt.to_period("M").astype(str)
        df["user_years"] = (
            (pd.to_datetime(df["add_time"]) - pd.to_datetime(df["regDate"])).dt.days / 365
        ).round(2)
        df["partition_month"] = pd.to_datetime(df["add_time"]).dt.strftime("%Y%m")
        return df

    # ------------------------------------------------------------------
    # 步骤 4: 写入宽表
    # ------------------------------------------------------------------

    def write_output(self, df: pd.DataFrame) -> None:
        out = df.reindex(columns=OUTPUT_COLS)
        db.bulk_upsert(config.DB_YUN, "analysis_wide_table_new", out)
        notifier.send_mail(
            subject="二手车价格平台清洗",
            body=f"{self.platform_name} 平台数据清洗更新成功",
        )

    # ------------------------------------------------------------------
    # 模板方法
    # ------------------------------------------------------------------

    def run(self) -> None:
        print(f"[{self.platform_name}] 开始清洗...")
        last_upd = self.get_last_update()
        print(f"[{self.platform_name}] 上次更新: {last_upd}")

        newdata = self.get_new_model_ids(last_upd)
        if newdata.empty:
            print(f"[{self.platform_name}] 无新增车型映射，跳过。")
            return

        raw = self.fetch_raw_data(newdata, last_upd)
        if raw.empty:
            print(f"[{self.platform_name}] 无新增数据，跳过。")
            return

        merged = self.merge_model_info(raw, newdata)
        cleaned = self.clean_common(merged)
        if cleaned.empty:
            print(f"[{self.platform_name}] 清洗后无有效数据，跳过。")
            return

        self.write_output(cleaned)
        print(f"[{self.platform_name}] 完成，写入 {len(cleaned)} 行。")
