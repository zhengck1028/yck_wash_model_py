"""
车型库源数据同步（对应 yck_spider_sync_old）

从本地爬虫库（DB_LOCAL）增量同步到 ODS 层（DB_ODS yck_ods），每周四执行。

同步内容：
  1. 新能源参数    config_che300_ev_info / config_autohome_ev_info
  2. 车300 详细配置 config_che300_detail_info
  3. 汽车之家详细配置 config_autohome_detail_info（含品牌/车系 ID 生成）
  4. 车300 主表    config_che300_major_info
  5. 各平台车型主表 config_autohome / souhu / czb / yiche / youxin / che58 / 12365 / autoowner / firstauto
"""
from __future__ import annotations

import re
from datetime import date, timedelta

import pandas as pd

from common import config, db, notifier
from common.logging_setup import get_logger

log = get_logger("vdatabase_sync")

# 本地内网爬虫库（192.168.0.10 yck-data-center）
DB_LOCAL = config.DB_LOCAL
# ODS 层（192.168.0.10 yck_ods）——ETL 结果写入此库
DB_ODS = config.DB_ODS

_WEEK_AGO = str(date.today() - timedelta(days=7))

_CAR_LEVEL_PATTERN = (
    r"紧凑型SUV|中型SUV|紧凑型车|中大型车|中型车|中大型SUV|大型SUV|大型车|微型车|小型车|MPV|"
    r"跑车|小型SUV|微面|皮卡|微卡|轻客|客车|紧凑型MPV|卡车|中大型MPV|其它|货车|重卡"
)

# ── 通用字段清洗工具 ───────────────────────────────────────────────────

def _normalize_auto(s: pd.Series) -> pd.Series:
    """对应 R dealFun_auto()，最终只保留 自动双离合/自动/手动/-"""
    s = s.astype(str)
    # CVT 系列
    s = s.str.replace(
        r"CVT无级变速|无级变速|自动.*CVT|CVT.*自动|-CVT|CVT", "自动", regex=True)
    # 双离合+手自动一体（先于单独双离合）
    s = s.str.replace(r"双离合.*手自动一体", "自动", regex=True)
    # 手自一体
    s = s.str.replace(r"手自一体型|手自动一体型|手自动一体|手自一体|tiptronic", "自动", regex=True)
    # DSG/DCT
    s = s.str.replace(r"DSG双离合|双离合器|双离合|G-DCT|DCT|DSG", "自动双离合", regex=True)
    # AT — 对应 R L103: [^a-zA-Z0-9]AT | (-| )[0-9]AT | AT( |-)
    s = s.str.replace(r"[^a-zA-Z0-9]AT|[-\s]\dAT|AT[\s-]", "自动", regex=True)
    # AT — 对应 R L104: AT[^a-zA-Z0-9] 全替换
    s = s.str.replace(r"AT(?![a-zA-Z0-9])", "自动", regex=True)
    # AMT/EMT/IMT
    s = s.str.replace(r"AMT智能手动版|AMT智能手动|AMT|EMT|IMT", "自动", regex=True)
    # MT
    s = s.str.replace(r"(?<![a-zA-Z])MT(?![a-zA-Z])|\dMT|MT[-\s]", "手动", regex=True)
    s = s.str.replace(r"(?i)^manual", "手动", regex=True)
    # 兜底提取：只保留 自动双离合/自动/手动，其余置 -
    extracted = s.str.extract(r"(自动双离合|自动|手动)")[0]
    return extracted.fillna("-")


def _normalize_discharge(model_name: pd.Series, discharge: pd.Series) -> pd.Series:
    """对应 R dealFun_discharge_standard()"""
    # 先从 model_name 提取标准编号
    extracted = model_name.astype(str).str.extract(
        r"((?:国|欧|京)(?:Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(?:型|)|京(?:5|五|V))"
    )[0]
    result = discharge.copy().astype(str)
    # 原字段无 国/欧/京 的，用从 model_name 提取的值覆盖
    missing = ~result.str.contains(r"国|欧|京", na=False)
    result[missing] = extracted[missing]
    # 两者都没提取到的置空（对应 R 的 <- NA）
    still_missing = ~result.str.contains(r"国|欧|京", na=False)
    result[still_missing] = None
    # 清洗：化油器→""，数字/罗马→汉字（顺序与 R 保持一致）
    rules = [
        (r"化油器", ""),
        (r"3|III|Ⅲ", "三"),
        (r"2|Ⅱ|II", "二"),
        (r"4|IV|Ⅳ", "四"),
        (r"6|VI|Ⅵ", "六"),
        (r"5|V|Ⅴ", "五"),
        (r"1|I|Ⅰ", "一"),
        (r"\+OBD|\)", ""),
        (r"带OBD", ""),
        (r"\(", "/"),
    ]
    for pat, repl in rules:
        result = result.str.replace(pat, repl, regex=True)
    return result


def _extract_liter(model_name: pd.Series, raw_liter: pd.Series) -> pd.Series:
    """对应 R: str_extract(liter,"[0-9][.][0-9]")，空则从 model_name 再提取"""
    liter = raw_liter.astype(str).str.extract(r"(\d+\.\d)")[0]
    still_null = liter.isna()
    liter[still_null] = model_name[still_null].astype(str).str.extract(r"(\d+\.\d)")[0]
    return liter


def _clean_model_year(s: pd.Series) -> pd.Series:
    """对应 R: gsub('其它|其他','') + model_year[=='0'] <- ''"""
    s = s.astype(str).str.replace(r"其它|其他", "", regex=True)
    s[s == "0"] = ""
    return s


def _normalize_ev_info(df: pd.DataFrame) -> pd.DataFrame:
    """对应 R dealFun_ev_info()"""
    # 电机类型：去空格/斜杠，"后" 前补 /
    if "ee_type" in df.columns:
        df["ee_type"] = df["ee_type"].astype(str).str.replace(r"/| ", "", regex=True)
        df["ee_type"] = df["ee_type"].str.replace(r"后", "/后", regex=True)
    # 充电时间无效标注 → -
    for col in ("ee_fast_charging_time", "ee_slow_charging_time"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"标配|选配|待查", "-", regex=True)
    # 电池类型：只保留已知类别，其余 → -
    if "ee_battery_type" in df.columns:
        extracted = df["ee_battery_type"].astype(str).str.extract(
            r"(三元镍钴锰酸锂|磷酸铁锂|三元锂|锂离子|钴酸锂|锰酸锂|镍钴锰|高压锂|镍氢)"
        )[0]
        df["ee_battery_type"] = extracted.apply(
            lambda v: f"{v}电池" if pd.notna(v) else "-"
        )
    # 续航字段（*_range 结尾）：去 标配/选配 和 km/公里 单位
    for col in [c for c in df.columns if c.endswith("range")]:
        df[col] = df[col].astype(str).str.replace(r"标配|选配", "-", regex=True)
        df[col] = df[col].str.replace(r"km|公里", "", regex=True)
    # 电池质保：汉字数字 → 阿拉伯数字，去空格/或
    if "ee_battery_quality_assurance" in df.columns:
        qa_map = [("三", "3"), ("四", "4"), ("五", "5"), ("六", "6"),
                  ("八", "8"), ("十", "10"), ("待查", "-")]
        for zh, ar in qa_map:
            df["ee_battery_quality_assurance"] = (
                df["ee_battery_quality_assurance"].astype(str).str.replace(zh, ar, regex=False)
            )
        df["ee_battery_quality_assurance"] = (
            df["ee_battery_quality_assurance"].str.replace(r"或| ", "", regex=True)
        )
    # 快充百分比：非 "-" 的值补 "%"
    if "ee_fast_charge" in df.columns:
        mask = df["ee_fast_charge"].astype(str) != "-"
        df.loc[mask, "ee_fast_charge"] = df.loc[mask, "ee_fast_charge"].astype(str) + "%"
    return df


def _get_pinyin_initial(brand_name: pd.Series) -> pd.Series:
    """品牌首字母——用 pypinyin 替代 R 的 Rcpp pinyin.cpp"""
    try:
        from pypinyin import lazy_pinyin, Style
        return brand_name.apply(
            lambda s: lazy_pinyin(str(s), style=Style.FIRST_LETTER)[0][0].upper()
            if s and str(s).strip() else "-"
        )
    except ImportError:
        return pd.Series(["-"] * len(brand_name), index=brand_name.index)

# ── 新能源参数同步 ────────────────────────────────────────────────────

def _sync_ev(src_table: str) -> str:
    """返回本次同步的 model_id 列表（逗号分隔），供跨表字段补充"""
    df = db.query(
        DB_LOCAL,
        f"""SELECT model_id, ee_type, ee_total_power, ee_total_torque,
                   ee_front_max_kw, ee_front_max_Nm, ee_after_max_kw, ee_after_max_Nm,
                   ee_system_integrated_kw, ee_system_integrated_Nm,
                   ee_driving_number, ee_layout, ee_battery_type,
                   ee_pure_electric_range, ee_battery_capacity, ee_power_consumption,
                   ee_battery_quality_assurance, ee_fast_charging_time,
                   ee_slow_charging_time, ee_fast_charge,
                   add_time, add_time AS update_time
            FROM {src_table}
            WHERE add_time >= '{_WEEK_AGO}'""",
    )
    if df.empty:
        return ""
    for col in df.columns:
        if col not in ("model_id", "add_time", "update_time"):
            df[col] = df[col].fillna("-").astype(str).str.replace(r"^$", "-", regex=True)
    df = _normalize_ev_info(df)
    db.bulk_upsert(DB_ODS, src_table, df)
    return ",".join(df["model_id"].astype(str).tolist())


def _fill_ev_cross(model_ids_c300: str, model_ids_au: str) -> None:
    """两表互补空缺的新能源核心字段"""
    vars_ = [
        "ee_type", "ee_driving_number", "ee_layout", "ee_battery_type",
        "ee_pure_electric_range", "ee_battery_capacity", "ee_power_consumption",
        "ee_battery_quality_assurance", "ee_fast_charging_time",
        "ee_slow_charging_time", "ee_fast_charge",
    ]
    tables = ["config_che300_ev_info", "config_autohome_ev_info"]
    ids = [model_ids_c300, model_ids_au]
    id_cols = ["id_che300", "id_autohome"]

    for j, (tbl, mid_str, id_col) in enumerate(zip(tables, ids, id_cols)):
        if not mid_str:
            continue
        other_tbl = tables[1 - j]
        other_id = id_cols[1 - j]
        for var in vars_:
            db.execute(
                DB_ODS,
                f"""UPDATE {tbl} m,
                    (SELECT a.model_id, c.{var}
                     FROM {tbl} a
                     INNER JOIN config_plat_id_match b ON a.model_id = b.{id_col}
                     INNER JOIN {other_tbl} c ON b.{other_id} = c.model_id
                     WHERE a.{var} = '-' AND c.{var} != '-' AND b.is_only_autohome = 1) d
                    SET m.{var} = d.{var}
                    WHERE m.model_id = d.model_id
                      AND m.model_id IN ({mid_str})""",
            )


# ── 车300 详细配置同步 ────────────────────────────────────────────────

def _sync_detail_c300() -> None:
    df = db.query(
        DB_LOCAL,
        f"""SELECT b.* FROM config_che300_major_info a
            INNER JOIN config_che300_detail_info b ON a.model_id = b.model_id
            WHERE b.update_time >= '{_WEEK_AGO}'""",
    )
    if "add_time" in df.columns:
        df = df.drop(columns=["add_time"])
    if not df.empty:
        db.bulk_upsert(DB_ODS, "config_che300_detail_info", df)


# ── 汽车之家详细配置同步 ──────────────────────────────────────────────

def _sync_detail_autohome() -> None:
    # config_autohome_detail_info 有 update_time，用它做增量
    df = db.query(
        DB_LOCAL,
        f"""SELECT b.autohome_id, b.emission, b.`level`, b.`engine`, b.gear_box,
                   b.`length`, b.width, b.height, b.body_structure, b.seat_number,
                   b.wheelbase, b.weight, b.trunk_volume, b.intake, b.cylinder_arrangement,
                   b.cylinders, b.admission_gear, b.max_ps, b.max_n_m, b.fuel,
                   b.fuel_grade, b.fuel_supply_system, b.environmental_standards_org,
                   b.drive_mode, b.front_suspension_type, b.rear_suspension_type,
                   b.power_type, b.car_boty_type, b.front_brake_type, b.rear_brake_type,
                   b.parking_brake_type, b.front_tire_specifications, b.rear_tire_specifications,
                   b.master_air_bag, b.sub_air_bag, b.front_side_air_bag, b.rear_side_air_bag,
                   b.front_head_air_bag, b.rear_head_air_bag, b.tire_pressure_monitoring,
                   b.ISOFIX_child_seat_interfaces, b.car_internal_lock, b.keyless_start_system,
                   b.abs, b.brake_assist, b.stability_control, b.variable_suspension,
                   b.electric_roof, b.panoramic_roof, b.multifunction_steering_wheel, b.cruise,
                   b.front_parking_radar, b.rear_parking_radar, b.reverse_video_image,
                   b.seat_material, b.electric_seat_memory, b.front_seat_heating,
                   b.rear_seat_heating, b.front_seat_ventilation, b.rear_seat_ventilation,
                   b.gps, b.low_beam_lamp, b.daytime_running_light, b.automatic_headlights,
                   b.front_fog_lamp, b.front_electric_window, b.rear_electric_window,
                   b.mirror_electric_adjustment, b.mirror_heating, b.air_conditioning_control_mode,
                   b.color_outside, b.color_outside_code, b.url, b.update_time
            FROM config_autohome_major_info_tmp a
            INNER JOIN config_autohome_detail_info b ON a.model_id = b.autohome_id
            WHERE a.model_year IS NOT NULL AND b.`level` != '-'
              AND b.update_time >= '{_WEEK_AGO}'""",
    )
    if not df.empty:
        db.bulk_upsert(DB_ODS, "config_autohome_detail_info", df)
    _sync_autohome_brand_series()


def _sync_autohome_brand_series() -> None:
    """品牌/车系 ID 增量同步到 config_autohome_yck_brand/series（从 DB_LOCAL 读，写 DB_ODS）"""
    # major_info_tmp 实际列名：brand_id / brand_letter / series_group_name / series_id / series_name
    brand_local = db.query(
        DB_LOCAL,
        "SELECT DISTINCT brand_id brandid, brand_letter Initial, brand_name FROM config_autohome_major_info_tmp",
    )
    if not brand_local.empty:
        try:
            brand_ods = db.query(DB_ODS, "SELECT brandid FROM config_autohome_yck_brand")
            existing_brands = set(brand_ods["brandid"].astype(str))
        except Exception:
            existing_brands = set()
        brand_add = brand_local[~brand_local["brandid"].astype(str).isin(existing_brands)].copy()
        if not brand_add.empty:
            brand_add["car_country"] = "无"
            db.bulk_insert(DB_ODS, "config_autohome_yck_brand", brand_add)

    series_local = db.query(
        DB_LOCAL,
        """SELECT DISTINCT a.series_id, a.series_group_name, a.series_name,
                  a.brand_id brandid, a.brand_name, b.`level` car_level
           FROM config_autohome_major_info_tmp a
           INNER JOIN config_autohome_detail_info b ON a.model_id = b.autohome_id
           WHERE b.`level` != '-'""",
    )
    if series_local.empty:
        return
    try:
        series_ods = db.query(DB_ODS, "SELECT series_id FROM config_autohome_yck_series")
        existing_series = set(series_ods["series_id"].astype(str))
    except Exception:
        existing_series = set()
    series_add = series_local[~series_local["series_id"].astype(str).isin(existing_series)].copy()
    if series_add.empty:
        return

    # 以下逻辑与原来相同
    brand_add_df = series_add  # 变量复用，只是命名对齐
    import_mask = brand_add_df["series_name"].str.contains("进口", na=False)
    brand_add_df.loc[import_mask, "series_group_name"] = (
        "进口" + brand_add_df.loc[import_mask, "series_group_name"]
    )
    brand_add_df["series_group_name"] = brand_add_df["series_group_name"].str.replace(
        r"进口进口进口|进口进口", "进口", regex=True
    )
    brand_add_df["series_name"] = (
        brand_add_df["series_name"]
        .str.replace(r"\(进口\)|\(停售\)", "", regex=True)
        .str.upper()
    )
    brand_add_df["is_import"] = brand_add_df["series_group_name"].str.contains("进口", na=False).astype(int)
    brand_add_df["car_level"] = brand_add_df["car_level"].str.extract(f"({_CAR_LEVEL_PATTERN})")[0].fillna("无")
    series_add = brand_add_df[["series_id", "series_group_name", "series_name", "brandid", "brand_name", "car_level", "is_import"]]
    db.bulk_upsert(DB_ODS, "config_autohome_yck_series", series_add)

    # （以下占位，原来后半段已移入上面）
    if False:
        series_add = None  # noqa
    if not series_add.empty:
        import_mask = series_add["series_name"].str.contains("进口", na=False)
        series_add.loc[import_mask, "series_group_name"] = (
            "进口" + series_add.loc[import_mask, "series_group_name"]
        )
        series_add["series_group_name"] = series_add["series_group_name"].str.replace(
            r"进口进口进口|进口进口", "进口", regex=True
        )
        series_add["series_name"] = (
            series_add["series_name"]
            .str.replace(r"\(进口\)|\(停售\)", "", regex=True)
            .str.upper()
        )
        series_add["is_import"] = series_add["series_group_name"].str.contains("进口", na=False).astype(int)
        series_add["car_level"] = series_add["car_level"].str.extract(f"({_CAR_LEVEL_PATTERN})")[0].fillna("无")
        series_add = series_add[["series_id", "series_group_name", "series_name", "brandid", "brand_name", "car_level", "is_import"]]
        db.bulk_upsert(DB_ODS, "config_autohome_yck_series", series_add)


# ── 车300 主表同步 ────────────────────────────────────────────────────

def _sync_che300_major() -> None:
    df = db.query(
        DB_LOCAL,
        f"""SELECT model_id, Initial, brandid, brand_name, series_group_name, series_id,
                   series_name, model_name, model_price, model_year, auto, liter,
                   liter_type, gear_type, discharge_standard, max_reg_year, min_reg_year,
                   car_level, seat_number, short_name, hl_configs, hl_configc, is_green, update_time
            FROM config_che300_major_info
            WHERE update_time >= '{_WEEK_AGO}'""",
    )
    if df.empty:
        return
    df["hl_configs"] = df["hl_configs"].str.replace(",", ";", regex=False)
    df["hl_configc"] = df["hl_configc"].str.replace(",", ";", regex=False)
    db.bulk_upsert(DB_ODS, "config_che300_major_info", df)


# ── 各平台车型主表同步（通用结构） ────────────────────────────────────

def _sync_platform_major(
    src_table: str,
    dst_table: str,
    sql: str,
    need_pinyin: bool = False,
    upsert: bool = True,
) -> None:
    df = db.query(DB_LOCAL, sql)
    if df.empty:
        return
    if need_pinyin and "Initial" in df.columns:
        df["Initial"] = _get_pinyin_initial(df["brand_name"])
    if "auto" in df.columns:
        model_col = "model_name" if "model_name" in df.columns else df.columns[0]
        df["auto"] = _normalize_auto((df[model_col] + df["auto"]).astype(str))
    if "liter" in df.columns:
        df["liter"] = _extract_liter(df.get("model_name", pd.Series("")), df["liter"])
    if "model_year" in df.columns:
        df["model_year"] = _clean_model_year(df["model_year"])
    if "discharge_standard" in df.columns and "model_name" in df.columns:
        df["discharge_standard"] = _normalize_discharge(df["model_name"], df["discharge_standard"])
    fn = db.bulk_upsert if upsert else db.bulk_insert
    fn(DB_ODS, dst_table, df)


def _sync_autohome_major() -> None:
    # major_info_tmp 无 update_time，用 add_time 做增量
    df = db.query(
        DB_LOCAL,
        f"""SELECT a.model_id, a.brand_id brandid, a.brand_name, a.brand_letter Initial,
                   a.series_group_name, a.series_id, a.series_name, a.model_year,
                   a.model_name, a.model_price, a.status,
                   b.emission liter, b.gear_box auto,
                   b.environmental_standards_org discharge_standard,
                   c.model_id is_green, b.fuel
            FROM config_autohome_major_info_tmp a
            INNER JOIN config_autohome_detail_info b ON a.model_id = b.autohome_id
            LEFT JOIN config_autohome_ev_info c ON a.model_id = c.model_id
            WHERE a.`status` != '即将销售'
              AND a.model_year IS NOT NULL AND a.model_price != 0
              AND b.`level` != '-'
              AND a.add_time >= '{_WEEK_AGO}'""",
    )
    if df.empty:
        return
    # 新能源标记
    df["is_green"] = df["is_green"].notna().astype(int)
    fuel_map = [
        (r".*纯电.*", "1"), (r".*油电.*", "2"),
        (r".*插电.*", "3"), (r".*增程.*", "4"),
    ]
    for pat, val in fuel_map:
        df.loc[df["fuel"].astype(str).str.match(pat), "fuel"] = val
    df["is_green"] = df.apply(
        lambda r: r["fuel"] if r["fuel"] in ("1", "2", "3", "4") else str(r["is_green"]), axis=1
    ).astype(int)
    df = df.drop(columns=["fuel"])
    # 变速箱
    df["auto"] = _normalize_auto((df["model_name"] + df["auto"].astype(str)).astype(str))
    df.loc[df["is_green"] == 1, "auto"] = "电动"
    # 在售状态
    df["status"] = df["status"].astype(str).str.extract(r"(停售|在售|即将销售)")[0]
    # 排量
    df["liter"] = _extract_liter(df["model_name"], df["liter"])
    df.loc[df["is_green"] == 1, "liter"] = "-"
    # 排放标准
    df["discharge_standard"] = _normalize_discharge(df["model_name"], df["discharge_standard"])
    df["is_check"] = 1
    db.bulk_upsert(DB_ODS, "config_autohome_major_info_tmp", df)


# ── 主入口 ────────────────────────────────────────────────────────────

def run() -> None:
    log.info("[vdatabase_sync] 开始...")

    # 1. 汽车之家新能源参数同步（DB_LOCAL 只有汽车之家数据，车300暂无）
    _sync_ev("config_autohome_ev_info")

    # 2. 汽车之家详细配置同步（含品牌/车系 ID 生成）
    _sync_detail_autohome()

    # 3. 汽车之家主表同步
    _sync_autohome_major()

    # # 搜狐
    # _sync_platform_major(
    #     "config_souhu_major_info", "config_souhu_major_info",
    #     f"""SELECT a.model_id, a.brand_id brandid, a.brand_name, a.initial Initial,
    #                a.series_group_name, a.series_id, a.series_name, a.model_year,
    #                a.model_name, a.model_price, a.volume liter, a.gear_box auto, '-' discharge_standard
    #         FROM config_souhu_major_info a WHERE a.update_time >= '{_WEEK_AGO}'""",
    # )

    # # 车置宝（INSERT IGNORE，无主键冲突更新需求）
    # _sync_platform_major(
    #     "config_chezhibao_major_info", "config_chezhibao_major_info",
    #     f"""SELECT a.model_id, a.brand_id brandid, a.brand brand_name, a.brand_letter Initial,
    #                a.company series_group_name, a.series_id, a.series_name, a.model_year,
    #                a.model_name, a.model_price, '-' liter, '-' auto, '-' discharge_standard
    #         FROM config_chezhibao_major_info a WHERE a.update_time >= '{_WEEK_AGO}'""",
    #     upsert=False,
    # )

    # # 易车
    # _sync_platform_major(
    #     "config_yiche_major_info", "config_yiche_major_info",
    #     f"""SELECT a.model_id, a.brand_id brandid, a.brand_name, a.init Initial,
    #                a.series_group series_group_name, a.series_id, a.series_name, a.model_year,
    #                a.model_name, a.model_price, '-' liter, a.gearbox auto, a.discharge_standard
    #         FROM config_yiche_major_info a WHERE a.update_time >= '{_WEEK_AGO}'""",
    #     upsert=False,
    # )

    # # 优信（需要拼音首字母）
    # _sync_platform_major(
    #     "config_youxin_major_info_tmp", "config_youxin_major_info_tmp",
    #     f"""SELECT a.model_id, a.brand_id brandid, a.brand_name, '-' Initial,
    #                '' series_group_name, a.series_id, a.series_name, a.model_year,
    #                a.model_name, '' model_price, '-' liter, '-' auto, '-' discharge_standard
    #         FROM config_youxin_major_info_tmp a WHERE a.update_time >= '{_WEEK_AGO}'""",
    #     need_pinyin=True, upsert=False,
    # )

    # # 58汽车（需要拼音首字母）
    # _sync_platform_major(
    #     "config_che58_major_info", "config_che58_major_info",
    #     f"""SELECT a.model_id, a.brand_id brandid, a.brand_name, '-' Initial,
    #                '' series_group_name, a.series_id, a.series_name, a.model_year,
    #                a.model_name, a.model_price, '-' liter, '-' auto, '-' discharge_standard
    #         FROM config_che58_major_info a WHERE a.update_time >= '{_WEEK_AGO}'""",
    #     need_pinyin=True, upsert=False,
    # )

    # 车主之家（DB_LOCAL 暂无此表，跳过）
    # _sync_platform_major(
    #     "config_autoowner_major_info_tmp", ...
    # )

    notifier.send_mail("车型库源数据同步完成", f"完成时间：{__import__('datetime').datetime.now()}")
    log.info("[vdatabase_sync] 完成。")


if __name__ == "__main__":
    run()
