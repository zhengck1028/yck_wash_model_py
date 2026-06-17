"""
其它数据同步（对应 fun_else_sync.R）

各部分触发时间：
  - 投诉数据          每月 1 日
  - 经销商报价        每月 28 日
  - 汽车销量          每月 28 日
  - 汽车口碑          每月 1 日
  - 经销商信息        每月 1 日
  - 车主裸车价（汽车之家）  每周四
  - 车主裸车价（易车）     每月 24 日

Lambda event 格式：
  {}                   → 按当天日期自动判断执行哪些任务
  {"task": "complaint"}  → 强制执行指定任务（便于手动补跑）

可用 task 值：complaint / discount / salesnum / koubei / dealer / owner_price_ah / owner_price_yiche
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta

import pandas as pd

from common import config, db, notifier

DB_LOCAL = config.DB_LOCAL
DB_YUN = config.DB_YUN

_TODAY = date.today()
_DAY = _TODAY.day
_MONTH_START = _TODAY.strftime("%Y-%m-01")


def _extract_city(s: pd.Series, config_distr: pd.DataFrame, config_distr_all: pd.DataFrame) -> pd.Series:
    """城市标准化（同 wash_platform base.py 逻辑）"""
    cities = config_distr["city"].tolist()
    pat = "|".join(re.escape(c) for c in cities)
    counties = config_distr_all["key_county"].tolist()
    county_pat = "|".join(re.escape(c) for c in counties)
    county_map = config_distr_all.drop_duplicates("key_county").set_index("key_county")["city"]

    def _match(val):
        if pd.isna(val):
            return None
        m = re.search(pat, str(val))
        if m:
            return m.group(0)
        m2 = re.search(county_pat, str(val))
        if m2:
            return county_map.get(m2.group(0))
        return None

    return s.apply(_match)


def _load_distr():
    distr = db.query(
        DB_YUN,
        """SELECT DISTINCT regional, b.province, a.key_municipal city
           FROM config_district a
           INNER JOIN config_district_regional b ON a.key_province = b.province""",
    )
    distr_all = db.query(
        DB_YUN,
        """SELECT DISTINCT regional, b.province, a.key_municipal city, a.key_county
           FROM config_district a
           INNER JOIN config_district_regional b ON a.key_province = b.province""",
    )
    return distr, distr_all


# ── 1. 投诉数据（每月 1 日） ────────────────────────────────────────

def sync_complaint() -> None:
    month_ago = str(_TODAY - timedelta(days=31))

    tables = {
        "spider_complain_12365auto": f"SELECT * FROM spider_complain_12365auto WHERE add_time > '{month_ago}'",
        "spider_complain_315qc": f"""SELECT id,series,brand,question,complain_time,
                                     DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                     FROM spider_complain_315qc
                                     WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{month_ago}'""",
        "spider_complain_qctsw": f"""SELECT id,series,brand,car_type,car_status,appeal,question,
                                     buy_time,complain_time,DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                     FROM spider_complain_qctsw
                                     WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{month_ago}'""",
        "spider_complain_qichemen": f"""SELECT id,series,brand,tag1,tag2,tag3,complain_time,
                                        DATE_FORMAT(add_time,'%Y-%m-%d') add_time
                                        FROM spider_complain_qichemen
                                        WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{month_ago}'""",
    }
    for tbl, sql in tables.items():
        df = db.query(DB_LOCAL, sql)
        if not df.empty:
            db.bulk_upsert(DB_YUN, tbl, df)

    # 车主之家投诉（需城市清洗）
    distr, distr_all = _load_distr()
    df = db.query(
        DB_LOCAL,
        f"""SELECT complain_id, series_id, city, tag1, des1, tag2, des2, tag3, des3,
                   tag4, des4, tag5, des5, tag6, des6,
                   DATE_FORMAT(complain_time,'%Y-%m-%d') complain_time,
                   DATE_FORMAT(update_time,'%Y-%m-%d') update_time
            FROM spider_complain_autoowner
            WHERE DATE_FORMAT(update_time,'%Y-%m-%d') > '{month_ago}'""",
    )
    if not df.empty:
        df["city"] = _extract_city(df["city"], distr, distr_all)
        df = df.merge(distr[["city", "province"]], on="city", how="inner")
        df = df[["complain_id", "series_id", "province", "city",
                  "tag1", "des1", "tag2", "des2", "tag3", "des3",
                  "tag4", "des4", "tag5", "des5", "tag6", "des6",
                  "complain_time", "update_time"]]
        db.bulk_upsert(DB_YUN, "spider_complain_autoowner", df)

    print("[else_sync] 投诉数据同步完成")


# ── 2. 经销商报价（每月 28 日） ──────────────────────────────────────

def sync_discount() -> None:
    stat_time = _TODAY.strftime("%Y-%m-15")
    df = db.query(DB_LOCAL, f"SELECT * FROM discount_rate_history WHERE stat_time = '{stat_time}'")
    if not df.empty:
        db.bulk_upsert(DB_YUN, "discount_rate_history", df)
        notifier.send_mail("数据同步-其它部分", "经销商报价 discount_rate_history 更新完毕")
    print("[else_sync] 经销商报价同步完成")


# ── 3. 汽车销量（每月 28 日） ────────────────────────────────────────

def sync_salesnum() -> None:
    # 搜狐
    df_souhu = db.query(
        DB_LOCAL,
        f"SELECT * FROM spider_salesnum_souhu WHERE add_time > '{_MONTH_START}'",
    )
    if not df_souhu.empty:
        db.bulk_upsert(DB_YUN, "spider_salesnum_souhu", df_souhu)

    # 车主之家
    month_ago = str(_TODAY - timedelta(days=31))
    df_ao = db.query(
        DB_LOCAL,
        f"""SELECT series_id, stat_date, salesNum, update_time add_time
            FROM spider_salesnum_autoowner
            WHERE update_time > '{month_ago}'""",
    )
    if not df_ao.empty:
        series_brand = db.query(
            DB_LOCAL,
            "SELECT DISTINCT series_id, series_name, brand_id, brand_name FROM config_autoowner_major_info_tmp",
        )
        series_brand["series_id"] = pd.to_numeric(series_brand["series_id"], errors="coerce")
        series_brand["brand_id"] = pd.to_numeric(series_brand["brand_id"], errors="coerce")
        df_ao["stat_date"] = pd.to_datetime(df_ao["stat_date"].astype(str) + "-01", errors="coerce")
        df_ao["series_id"] = pd.to_numeric(df_ao["series_id"], errors="coerce")
        df_ao = df_ao.merge(series_brand, on="series_id", how="inner")
        df_ao = df_ao[["brand_id", "brand_name", "series_id", "series_name", "stat_date", "salesNum", "add_time"]]
        db.bulk_upsert(DB_YUN, "spider_salesnum_autoowner", df_ao)

    print("[else_sync] 销量数据同步完成")


# ── 4. 汽车口碑（每月 1 日） ─────────────────────────────────────────

def sync_koubei() -> None:
    since = (_TODAY - timedelta(days=5)).strftime("%Y-%m-01")
    df = db.query(
        DB_LOCAL,
        f"""SELECT id, eid, model_id, isbattery,
                   IF(drivenKilometers_appends>=drivekilometer, drivenKilometers_appends, drivekilometer) miles,
                   boughtdate,
                   SUBSTR(boughtcity_id FROM 1 FOR 2) province_id,
                   SUBSTR(boughtcity_id FROM 1 FOR 4) city_id,
                   visitcount, helpfulcount, commentcount, boughtPrice,
                   score_spaceScene space, score_powerScene power,
                   score_maneuverabilityScene control, score_oilScene oilconsumption,
                   score_batteryScene eleconsumption, score_comfortablenessScene comfortableness,
                   score_apperanceScene apperance, score_internalScene interior,
                   score_costefficientScene costefficient, satisfaction,
                   append_time comment_time, DATE_FORMAT(add_time,'%Y-%m-%d') add_time
            FROM spider_koubei_autohome
            WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{since}'""",
    )
    if df.empty:
        print("[else_sync] 口碑：无新数据")
        return

    model_info = db.query(
        DB_LOCAL,
        "SELECT model_id, model_name, brand_id, brand_name, series_id, series_name, model_year, model_price FROM config_autohome_major_info_tmp",
    )
    district_code = db.query(
        DB_LOCAL,
        """SELECT SUBSTR(city_code,1,2) province_id, SUBSTR(city_code,1,4) city_id,
                  key_province province, MIN(key_municipal) city
           FROM config_district
           GROUP BY SUBSTR(city_code,1,4), key_province""",
    )
    model_info["model_id"] = pd.to_numeric(model_info["model_id"], errors="coerce")
    df["model_id"] = pd.to_numeric(df["model_id"], errors="coerce")

    prov_map = district_code[["province_id", "province"]].drop_duplicates()
    city_map = district_code[["city_id", "city"]].drop_duplicates()

    df = (
        df.merge(model_info, on="model_id", how="inner")
          .merge(prov_map, on="province_id", how="inner")
          .merge(city_map, on="city_id", how="left")
    )
    df["boughtdate"] = (df["boughtdate"].astype(str) + "-01").apply(
        lambda s: pd.to_datetime(s, errors="coerce")
    )
    df = df[["id", "eid", "model_id", "brand_id", "brand_name", "series_id", "series_name",
             "model_year", "model_name", "model_price", "isbattery",
             "boughtdate", "boughtPrice", "province", "city", "miles",
             "visitcount", "helpfulcount", "commentcount",
             "space", "power", "control", "oilconsumption", "eleconsumption",
             "comfortableness", "apperance", "interior", "costefficient",
             "satisfaction", "comment_time", "add_time"]]
    db.bulk_upsert(DB_YUN, "spider_koubei_autohome", df)
    print("[else_sync] 口碑数据同步完成")


# ── 5. 经销商信息（每月 1 日） ───────────────────────────────────────

def sync_dealer() -> None:
    since = (_TODAY - timedelta(days=5)).strftime("%Y-%m-01")
    dealer_tables = {
        "spider_dealer_autohome": f"""SELECT id,dealer_id,shop_4s,name,brand,linkPhone,
                                      province_name,city_name,update_time
                                      FROM spider_dealer_autohome
                                      WHERE DATE_FORMAT(update_time,'%Y-%m-%d') > '{since}'""",
        "spider_dealer_bitauto":  f"""SELECT id,dealer_id,shop_4s,name,brand,linkPhone,
                                      province_name,city_name,update_time
                                      FROM spider_dealer_bitauto
                                      WHERE DATE_FORMAT(update_time,'%Y-%m-%d') > '{since}'""",
        "spider_dealer_ownerhome": f"""SELECT id,dealer_id,shop_4s,name,brand,linkPhone,
                                       province_name,city_name,update_time
                                       FROM spider_dealer_ownerhome
                                       WHERE DATE_FORMAT(update_time,'%Y-%m-%d') > '{since}'""",
    }
    for tbl, sql in dealer_tables.items():
        df = db.query(DB_LOCAL, sql)
        if tbl == "spider_dealer_ownerhome" and not df.empty:
            df["city_name"] = df["city_name"].str.replace("市", "", regex=False)
        if not df.empty:
            db.bulk_upsert(DB_YUN, tbl, df)
            notifier.send_mail("数据同步-其它部分", f"经销商 {tbl} 更新完毕")
    print("[else_sync] 经销商信息同步完成")


# ── 6. 车主裸车价·汽车之家（每周四） ─────────────────────────────────

def sync_owner_price_autohome() -> None:
    since = (_TODAY - timedelta(days=5)).strftime("%Y-%m-01")
    df = db.query(
        DB_LOCAL,
        f"""SELECT id, model_id, series_id, series_name, model_name,
                   boughtaddress city, boughtdate, owner_price_id owner_id,
                   model_price, bare_price, fullprice, purchase_tax,
                   commercial_insure, vehicle_tax, high_insure, card_fee, add_time
            FROM spider_ownerprice_autohome
            WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{since}'""",
    )
    if df.empty:
        print("[else_sync] 汽车之家裸车价：无新数据")
        return

    distr, distr_all = _load_distr()
    df["city_clean"] = _extract_city(df["city"], distr, distr_all)
    df["province"] = _extract_city(df["city"], distr.rename(columns={"city": "province"})[["province"]], distr_all)

    # 省份单独提取
    prov_pat = "|".join(re.escape(p) for p in distr["province"].unique())
    df["province"] = df["city"].str.extract(f"({prov_pat})")[0]

    df["city"] = df["city_clean"]
    df = df.drop(columns=["city_clean"])
    df["boughtdate"] = pd.to_datetime(df["boughtdate"], errors="coerce")
    df = df[
        (df["model_price"] != 0) & (df["bare_price"] != 0) &
        ((df["model_price"] - df["bare_price"]).abs() / df["bare_price"] <= 3) &
        ((df["model_price"] - df["bare_price"]).abs() / df["model_price"] <= 2)
    ]
    df = df[["id", "model_id", "series_id", "series_name", "model_name", "model_price",
             "province", "city", "boughtdate", "owner_id", "bare_price", "fullprice",
             "purchase_tax", "commercial_insure", "vehicle_tax", "high_insure", "card_fee", "add_time"]]
    db.bulk_upsert(DB_YUN, "spider_ownerprice_autohome", df)
    print("[else_sync] 汽车之家裸车价同步完成")


# ── 7. 车主裸车价·易车（每月 24 日） ─────────────────────────────────

def sync_owner_price_yiche() -> None:
    since = (_TODAY - timedelta(days=8)).strftime("%Y-%m-20")
    df = db.query(
        DB_LOCAL,
        f"""SELECT id, brand_name, series_name, model_name, buy_time, city,
                   ROUND(guidance_price/10000, 2) model_price,
                   ROUND(naked_price/10000, 2) bare_price, add_time
            FROM spider_nakedprice_yiche
            WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{since}'""",
    )
    if df.empty:
        print("[else_sync] 易车裸车价：无新数据")
        return

    distr, distr_all = _load_distr()
    df["city_clean"] = _extract_city(df["city"], distr, distr_all)
    df = df.merge(distr[["city", "province"]], left_on="city_clean", right_on="city", how="inner").drop(columns=["city_x", "city_y"])
    df = df.rename(columns={"city_clean": "city"})
    df["buy_time"] = pd.to_datetime(df["buy_time"], errors="coerce")
    df["model_year"] = df["model_name"].str.extract(r"([12]\d{3})款|(\d{2})款")[0].fillna(
        df["model_name"].str.extract(r"([12]\d{3})款|(\d{2})款")[1]
    ).str.replace("款", "", regex=False)
    df["brand_name"] = df["brand_name"].str.upper()
    df["series_name"] = df["series_name"].str.upper()
    df = df[
        (df["model_price"] != 0) & (df["bare_price"] != 0) &
        ((df["model_price"] - df["bare_price"]).abs() / df["bare_price"] <= 3) &
        ((df["model_price"] - df["bare_price"]).abs() / df["model_price"] <= 2)
    ]
    df = df[["id", "brand_name", "series_name", "model_year", "model_name",
             "model_price", "province", "city", "buy_time", "bare_price", "add_time"]]
    db.bulk_upsert(DB_YUN, "spider_nakedprice_yiche", df)
    print("[else_sync] 易车裸车价同步完成")


# ── 主入口 ────────────────────────────────────────────────────────────

_TASK_MAP = {
    "complaint":       sync_complaint,
    "discount":        sync_discount,
    "salesnum":        sync_salesnum,
    "koubei":          sync_koubei,
    "dealer":          sync_dealer,
    "owner_price_ah":  sync_owner_price_autohome,
    "owner_price_yiche": sync_owner_price_yiche,
}

# 按日期自动判断哪些任务需要执行
_SCHEDULE: dict[str, list[str]] = {
    "day_1":    ["complaint", "koubei", "dealer"],
    "day_28":   ["discount", "salesnum"],
    "day_24":   ["owner_price_yiche"],
    "thursday": ["owner_price_ah"],
}


def run(task: str | None = None) -> None:
    if task:
        tasks = [task]
    else:
        tasks = []
        if _DAY == 1:
            tasks += _SCHEDULE["day_1"]
        if _DAY == 28:
            tasks += _SCHEDULE["day_28"]
        if _DAY == 24:
            tasks += _SCHEDULE["day_24"]
        if _TODAY.weekday() == 3:  # 周四
            tasks += _SCHEDULE["thursday"]

    if not tasks:
        print("[else_sync] 今日无需执行任何任务。")
        return

    for t in tasks:
        fn = _TASK_MAP.get(t)
        if fn is None:
            print(f"[else_sync] 未知任务: {t}")
            continue
        try:
            fn()
        except Exception as e:
            import traceback
            notifier.send_mail(f"数据同步-其它部分 {t} 异常", traceback.format_exc())


if __name__ == "__main__":
    run()
