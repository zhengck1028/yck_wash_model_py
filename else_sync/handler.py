"""
其它数据同步（PySpark 实现）

各部分触发时间：
  - 投诉数据          每月 1 日
  - 经销商报价        每月 28 日
  - 汽车销量          每月 28 日
  - 汽车口碑          每月 1 日
  - 经销商信息        每月 1 日
  - 车主裸车价（汽车之家）  每周四
  - 车主裸车价（易车）     每月 24 日

入口 run(task=None)：
  None     → 按当天日期自动判断执行哪些任务
  "xxx"    → 强制执行指定任务（便于手动补跑）

读取走 Spark JDBC，写回走临时表 MERGE（common.spark.write_replace）。
城市标准化等带正则的清洗用 Spark Column 表达式实现。
"""
from __future__ import annotations

import re
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config, db, notifier
from common.spark import get_spark, read_sql, write_replace

DB_LOCAL = config.DB_LOCAL
DB_YUN = config.DB_YUN

_TODAY = date.today()
_DAY = _TODAY.day
_MONTH_START = _TODAY.strftime("%Y-%m-01")


# ── 城市标准化 ─────────────────────────────────────────────────────────

def _load_distr() -> tuple[DataFrame, DataFrame]:
    distr = read_sql(
        DB_YUN,
        """SELECT DISTINCT regional, b.province, a.key_municipal city
           FROM config_district a
           INNER JOIN config_district_regional b ON a.key_province = b.province""",
    )
    distr_all = read_sql(
        DB_YUN,
        """SELECT DISTINCT regional, b.province, a.key_municipal city, a.key_county
           FROM config_district a
           INNER JOIN config_district_regional b ON a.key_province = b.province""",
    )
    return distr, distr_all


def _city_extract_expr(col: F.Column, cities: list[str]) -> F.Column:
    """从文本中提取首个命中的城市名（对应 pandas re.search(城市pattern)）。"""
    pat = "|".join(re.escape(c) for c in cities)
    # regexp_extract 取第一个匹配；无匹配返回 ""，转 None
    ext = F.regexp_extract(col.cast("string"), f"({pat})", 1)
    return F.when(ext == "", None).otherwise(ext)


def _add_city_clean(sdf: DataFrame, src_col: str, distr: DataFrame, distr_all: DataFrame) -> DataFrame:
    """城市标准化：先按城市名匹配，未命中再按县级映射到城市。

    对应 else_sync 原 _extract_city。distr/distr_all 是小表，collect 到 driver
    构造正则（与 pandas 版一致的「整列正则匹配」语义）。
    """
    cities = [r["city"] for r in distr.select("city").distinct().collect() if r["city"]]
    county_rows = distr_all.select("key_county", "city").distinct().collect()
    counties = [r["key_county"] for r in county_rows if r["key_county"]]
    county_to_city = {r["key_county"]: r["city"] for r in county_rows if r["key_county"]}

    col = F.col(src_col)
    city_hit = _city_extract_expr(col, cities)

    # 县级回退：未命中城市时，从县名映射回城市
    county_pat = "|".join(re.escape(c) for c in counties)
    county_ext = F.regexp_extract(col.cast("string"), f"({county_pat})", 1)
    # 用 map 把县名翻成城市
    mapping = F.create_map([F.lit(x) for kv in county_to_city.items() for x in kv]) if county_to_city else None
    county_city = mapping[county_ext] if mapping is not None else F.lit(None)

    result = F.when(city_hit.isNotNull(), city_hit).otherwise(
        F.when(county_ext != "", county_city).otherwise(None)
    )
    return sdf.withColumn(src_col, result)


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
        sdf = read_sql(DB_LOCAL, sql)
        if len(sdf.head(1)):
            write_replace(DB_YUN, tbl, sdf)

    # 车主之家投诉（需城市清洗）
    distr, distr_all = _load_distr()
    sdf = read_sql(
        DB_LOCAL,
        f"""SELECT complain_id, series_id, city, tag1, des1, tag2, des2, tag3, des3,
                   tag4, des4, tag5, des5, tag6, des6,
                   DATE_FORMAT(complain_time,'%Y-%m-%d') complain_time,
                   DATE_FORMAT(update_time,'%Y-%m-%d') update_time
            FROM spider_complain_autoowner
            WHERE DATE_FORMAT(update_time,'%Y-%m-%d') > '{month_ago}'""",
    )
    if len(sdf.head(1)):
        sdf = _add_city_clean(sdf, "city", distr, distr_all)
        sdf = sdf.join(distr.select("city", "province").distinct(), on="city", how="inner")
        sdf = sdf.select(
            "complain_id", "series_id", "province", "city",
            "tag1", "des1", "tag2", "des2", "tag3", "des3",
            "tag4", "des4", "tag5", "des5", "tag6", "des6",
            "complain_time", "update_time",
        )
        write_replace(DB_YUN, "spider_complain_autoowner", sdf)
    print("[else_sync] 投诉数据同步完成")


# ── 2. 经销商报价（每月 28 日） ──────────────────────────────────────

def sync_discount() -> None:
    stat_time = _TODAY.strftime("%Y-%m-15")
    sdf = read_sql(DB_LOCAL, f"SELECT * FROM discount_rate_history WHERE stat_time = '{stat_time}'")
    if len(sdf.head(1)):
        write_replace(DB_YUN, "discount_rate_history", sdf)
        notifier.send_mail("数据同步-其它部分", "经销商报价 discount_rate_history 更新完毕")
    print("[else_sync] 经销商报价同步完成")


# ── 3. 汽车销量（每月 28 日） ────────────────────────────────────────

def sync_salesnum() -> None:
    # 搜狐
    df_souhu = read_sql(DB_LOCAL, f"SELECT * FROM spider_salesnum_souhu WHERE add_time > '{_MONTH_START}'")
    if len(df_souhu.head(1)):
        write_replace(DB_YUN, "spider_salesnum_souhu", df_souhu)

    # 车主之家
    month_ago = str(_TODAY - timedelta(days=31))
    df_ao = read_sql(
        DB_LOCAL,
        f"""SELECT series_id, stat_date, salesNum, update_time add_time
            FROM spider_salesnum_autoowner
            WHERE update_time > '{month_ago}'""",
    )
    if len(df_ao.head(1)):
        series_brand = read_sql(
            DB_LOCAL,
            "SELECT DISTINCT series_id, series_name, brand_id, brand_name FROM config_autoowner_major_info_tmp",
        )
        series_brand = (
            series_brand
            .withColumn("series_id", F.col("series_id").cast("long"))
            .withColumn("brand_id", F.col("brand_id").cast("long"))
        )
        df_ao = (
            df_ao
            .withColumn("stat_date", F.to_date(F.concat(F.col("stat_date").cast("string"), F.lit("-01"))))
            .withColumn("series_id", F.col("series_id").cast("long"))
            .join(series_brand, on="series_id", how="inner")
            .select("brand_id", "brand_name", "series_id", "series_name", "stat_date", "salesNum", "add_time")
        )
        write_replace(DB_YUN, "spider_salesnum_autoowner", df_ao)
    print("[else_sync] 销量数据同步完成")


# ── 4. 汽车口碑（每月 1 日） ─────────────────────────────────────────

def sync_koubei() -> None:
    since = (_TODAY - timedelta(days=5)).strftime("%Y-%m-01")
    sdf = read_sql(
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
    if len(sdf.head(1)) == 0:
        print("[else_sync] 口碑：无新数据")
        return

    model_info = read_sql(
        DB_LOCAL,
        "SELECT model_id, model_name, brand_id, brand_name, series_id, series_name, model_year, model_price FROM config_autohome_major_info_tmp",
    ).withColumn("model_id", F.col("model_id").cast("long"))
    district_code = read_sql(
        DB_LOCAL,
        """SELECT SUBSTR(city_code,1,2) province_id, SUBSTR(city_code,1,4) city_id,
                  key_province province, MIN(key_municipal) city
           FROM config_district
           GROUP BY SUBSTR(city_code,1,4), key_province""",
    )
    prov_map = district_code.select("province_id", "province").distinct()
    city_map = district_code.select("city_id", "city").distinct()

    sdf = (
        sdf.withColumn("model_id", F.col("model_id").cast("long"))
        .join(model_info, on="model_id", how="inner")
        .join(prov_map, on="province_id", how="inner")
        .join(city_map, on="city_id", how="left")
    )
    sdf = sdf.withColumn("boughtdate", F.to_date(F.concat(F.col("boughtdate").cast("string"), F.lit("-01"))))
    sdf = sdf.select(
        "id", "eid", "model_id", "brand_id", "brand_name", "series_id", "series_name",
        "model_year", "model_name", "model_price", "isbattery",
        "boughtdate", "boughtPrice", "province", "city", "miles",
        "visitcount", "helpfulcount", "commentcount",
        "space", "power", "control", "oilconsumption", "eleconsumption",
        "comfortableness", "apperance", "interior", "costefficient",
        "satisfaction", "comment_time", "add_time",
    )
    write_replace(DB_YUN, "spider_koubei_autohome", sdf)
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
        sdf = read_sql(DB_LOCAL, sql)
        if tbl == "spider_dealer_ownerhome" and len(sdf.head(1)):
            sdf = sdf.withColumn("city_name", F.regexp_replace(F.col("city_name"), "市", ""))
        if len(sdf.head(1)):
            write_replace(DB_YUN, tbl, sdf)
            notifier.send_mail("数据同步-其它部分", f"经销商 {tbl} 更新完毕")
    print("[else_sync] 经销商信息同步完成")


# ── 6. 车主裸车价·汽车之家（每周四） ─────────────────────────────────

def sync_owner_price_autohome() -> None:
    since = (_TODAY - timedelta(days=5)).strftime("%Y-%m-01")
    sdf = read_sql(
        DB_LOCAL,
        f"""SELECT id, model_id, series_id, series_name, model_name,
                   boughtaddress city, boughtdate, owner_price_id owner_id,
                   model_price, bare_price, fullprice, purchase_tax,
                   commercial_insure, vehicle_tax, high_insure, card_fee, add_time
            FROM spider_ownerprice_autohome
            WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{since}'""",
    )
    if len(sdf.head(1)) == 0:
        print("[else_sync] 汽车之家裸车价：无新数据")
        return

    distr, distr_all = _load_distr()
    provinces = [r["province"] for r in distr.select("province").distinct().collect() if r["province"]]
    prov_pat = "|".join(re.escape(p) for p in provinces)

    # 省份：从原始 city 文本提取
    sdf = sdf.withColumn("province", F.regexp_extract(F.col("city").cast("string"), f"({prov_pat})", 1))
    sdf = sdf.withColumn("province", F.when(F.col("province") == "", None).otherwise(F.col("province")))
    # 城市标准化（覆盖 city 列）
    sdf = _add_city_clean(sdf, "city", distr, distr_all)
    sdf = sdf.withColumn("boughtdate", F.to_date(F.col("boughtdate")))

    # 价格合理性过滤（与 pandas 版一致）
    mp = F.col("model_price").cast("double")
    bp = F.col("bare_price").cast("double")
    sdf = sdf.filter(
        (mp != 0) & (bp != 0)
        & (F.abs(mp - bp) / bp <= 3)
        & (F.abs(mp - bp) / mp <= 2)
    )
    sdf = sdf.select(
        "id", "model_id", "series_id", "series_name", "model_name", "model_price",
        "province", "city", "boughtdate", "owner_id", "bare_price", "fullprice",
        "purchase_tax", "commercial_insure", "vehicle_tax", "high_insure", "card_fee", "add_time",
    )
    write_replace(DB_YUN, "spider_ownerprice_autohome", sdf)
    print("[else_sync] 汽车之家裸车价同步完成")


# ── 7. 车主裸车价·易车（每月 24 日） ─────────────────────────────────

def sync_owner_price_yiche() -> None:
    since = (_TODAY - timedelta(days=8)).strftime("%Y-%m-20")
    sdf = read_sql(
        DB_LOCAL,
        f"""SELECT id, brand_name, series_name, model_name, buy_time, city,
                   ROUND(guidance_price/10000, 2) model_price,
                   ROUND(naked_price/10000, 2) bare_price, add_time
            FROM spider_nakedprice_yiche
            WHERE DATE_FORMAT(add_time,'%Y-%m-%d') > '{since}'""",
    )
    if len(sdf.head(1)) == 0:
        print("[else_sync] 易车裸车价：无新数据")
        return

    distr, distr_all = _load_distr()
    sdf = _add_city_clean(sdf, "city", distr, distr_all)
    sdf = sdf.join(distr.select("city", "province").distinct(), on="city", how="inner")
    sdf = sdf.withColumn("buy_time", F.to_date(F.col("buy_time")))
    # 年款：从 model_name 提取 4 位或 2 位年款
    yr4 = F.regexp_extract(F.col("model_name"), r"([12]\d{3})款", 1)
    yr2 = F.regexp_extract(F.col("model_name"), r"(\d{2})款", 1)
    sdf = sdf.withColumn("model_year", F.when(yr4 != "", yr4).otherwise(yr2))
    sdf = sdf.withColumn("brand_name", F.upper(F.col("brand_name")))
    sdf = sdf.withColumn("series_name", F.upper(F.col("series_name")))

    mp = F.col("model_price").cast("double")
    bp = F.col("bare_price").cast("double")
    sdf = sdf.filter(
        (mp != 0) & (bp != 0)
        & (F.abs(mp - bp) / bp <= 3)
        & (F.abs(mp - bp) / mp <= 2)
    )
    sdf = sdf.select(
        "id", "brand_name", "series_name", "model_year", "model_name",
        "model_price", "province", "city", "buy_time", "bare_price", "add_time",
    )
    write_replace(DB_YUN, "spider_nakedprice_yiche", sdf)
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

_SCHEDULE: dict[str, list[str]] = {
    "day_1":    ["complaint", "koubei", "dealer"],
    "day_28":   ["discount", "salesnum"],
    "day_24":   ["owner_price_yiche"],
    "thursday": ["owner_price_ah"],
}


def run(task: str | None = None) -> None:
    get_spark()  # 触发 SparkSession 初始化
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
        except Exception:
            import traceback
            notifier.send_mail(f"数据同步-其它部分 {t} 异常", traceback.format_exc())


if __name__ == "__main__":
    run()
