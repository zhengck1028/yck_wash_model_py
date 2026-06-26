from __future__ import annotations

import re

import pandas as pd

from common import config, db, notifier
from common.logging_setup import get_logger

log = get_logger("autohome_match")

# 排放标准规范化映射（顺序敏感，长串先匹配）
_EMISSION_MAP = [
    (r"\+OBD|\)", ""),
    (r"3|III|Ⅲ", "三"),
    (r"2|Ⅱ|II", "二"),
    (r"4|IV|Ⅳ", "四"),
    (r"6|VI|Ⅵ", "六"),
    (r"5|V", "五"),
    (r"\(", "/"),
    (r"未知", "-"),
]

_FRONT_COLS = [
    "master_air_bag", "front_side_air_bag", "front_head_air_bag",
    "front_parking_radar", "front_seat_heating", "front_seat_ventilation",
    "front_electric_window",
]
_REAR_COLS = [
    "sub_air_bag", "rear_side_air_bag", "rear_head_air_bag",
    "rear_parking_radar", "rear_seat_heating", "rear_seat_ventilation",
    "rear_electric_window",
]

_CONFIG_COLS = [
    "master_air_bag", "sub_air_bag", "front_side_air_bag", "rear_side_air_bag",
    "front_head_air_bag", "rear_head_air_bag", "front_parking_radar", "rear_parking_radar",
    "front_seat_heating", "rear_seat_heating", "front_seat_ventilation", "rear_seat_ventilation",
    "electric_roof", "panoramic_roof", "multifunction_steering_wheel", "cruise",
    "front_electric_window", "rear_electric_window", "mirror_electric_adjustment",
    "mirror_heating", "air_conditioning_control_mode",
    "color_outside", "color_outside_code",
    "gps", "low_beam_lamp", "daytime_running_light", "automatic_headlights", "front_fog_lamp",
    "tire_pressure_monitoring", "ISOFIX_child_seat_interfaces", "car_internal_lock",
    "keyless_start_system", "abs", "brake_assist", "stability_control", "variable_suspension",
    "seat_material", "electric_seat_memory",
]

YCK2_RENAME = [
    "autohome_id", "emission", "level", "engine", "gear_box", "length", "width", "height",
    "body_structure", "seat_number", "wheelbase", "weight", "trunk_volume", "intake",
    "cylinder_arrangement", "cylinders", "admission_gear", "max_ps", "max_n-m",
    "fuel", "fuel_grade", "fuel_supply_system", "environmental_standards_org",
    "drive_mode", "front_suspension_type", "rear_suspension_type", "power_type",
    "car_boty_type", "front_brake_type", "rear_brake_type", "parking_brake_type",
    "front_tire_specifications", "rear_tire_specifications",
    "master_air_bag", "sub_air_bag", "front_side_air_bag", "rear_side_air_bag",
    "front_head_air_bag", "rear_head_air_bag",
    "tire_pressure_monitoring", "ISOFIX_child_seat_interfaces", "car_internal_lock",
    "keyless_start_system", "abs", "brake_assist", "stability_control", "variable_suspension",
    "electric_roof", "panoramic_roof", "multifunction_steering_wheel", "cruise",
    "front_parking_radar", "rear_parking_radar", "reverse_video_image",
    "seat_material", "electric_seat_memory",
    "front_seat_heating", "rear_seat_heating", "front_seat_ventilation", "rear_seat_ventilation",
    "gps", "low_beam_lamp", "daytime_running_light", "automatic_headlights", "front_fog_lamp",
    "front_electric_window", "rear_electric_window",
    "mirror_electric_adjustment", "mirror_heating", "air_conditioning_control_mode",
    "color_outside", "color_outside_code", "hl_configs", "hl_configc",
]

DB_ODS = config.DB_ODS
DB_IT = config.DB_IT


def _normalize_emission(s: str) -> str:
    for pattern, repl in _EMISSION_MAP:
        s = re.sub(pattern, repl, s)
    return s


def _split_pair(df: pd.DataFrame, front_cols: list, rear_cols: list) -> pd.DataFrame:
    for fc, rc in zip(front_cols, rear_cols):
        if fc in df.columns:
            df[fc] = df[fc].astype(str).str.replace(r"\?\\/.*|\\/.*", "", regex=True)
        if rc in df.columns:
            df[rc] = df[rc].astype(str).str.replace(r".*\\/\\?|.*\\/", "", regex=True)
    return df


def _normalize_config_flag(df: pd.DataFrame) -> pd.DataFrame:
    for col in [c for c in _CONFIG_COLS if c in df.columns]:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.replace(r"前标配|后标配|主标配|副标配|标配", "Y", regex=True)
        df[col] = df[col].str.replace(r"前选配|后选配|主选配|副选配|选配", "-", regex=True)
        df[col] = df[col].str.replace(r"前无|后无|主无|副无|无", "N", regex=True)
        df[col] = df[col].str.replace(r"前-|后-|主-|副-|-", "N", regex=True)
    return df


def _id_str(series: pd.Series) -> pd.Series:
    """把 ID 列转成干净整数字符串 Series（保留原索引，NaN→''）。

    DB 取回的整数列若含 NULL，pandas 会整列转 float64，astype(str) 产生
    '12345.0' 这种带 .0 的脏值，导致与纯整数 ID 的差集判断全部漏匹配。
    这里统一转 numeric → Int64（可空整数）→ str，NaN 变成空串不会误匹配。
    """
    nums = pd.to_numeric(series, errors="coerce").astype("Int64")
    return nums.astype(str).replace("<NA>", "")


def _id_set(series: pd.Series) -> set[str]:
    """规范化 ID 列为干净整数字符串集合（丢弃 NaN）。"""
    nums = pd.to_numeric(series, errors="coerce").dropna()
    return set(nums.astype("int64").astype(str))


def run() -> None:
    # ── 品牌同步 ──────────────────────────────────────────────────────────
    log.info("[1/7] 品牌同步...")
    yck_brand = db.query(DB_ODS, "SELECT * FROM config_autohome_yck_brand")
    if not yck_brand.empty:
        it_brand_ids = db.query(DB_IT, "SELECT brandid FROM yck_car_basic_brand")
        existing = set(it_brand_ids["brandid"].astype(str))
        yck_brand = yck_brand[~yck_brand["brandid"].astype(str).isin(existing)]
    if not yck_brand.empty:
        db.bulk_upsert(DB_IT, "yck_car_basic_brand", yck_brand)
        log.info(f"  → 新增品牌 {len(yck_brand)} 条")
    else:
        log.info("  → 无新品牌")

    # ── 车系同步 ──────────────────────────────────────────────────────────
    log.info("[2/7] 车系同步...")
    yck_series = db.query(
        DB_ODS,
        "SELECT series_id, series_group_name, series_name, brandid, brand_name, car_level, is_import FROM config_autohome_yck_series",
    )
    if not yck_series.empty:
        it_series_ids = db.query(DB_IT, "SELECT series_id FROM yck_car_basic_series")
        existing_s = set(it_series_ids["series_id"].astype(str))
        yck_series = yck_series[~yck_series["series_id"].astype(str).isin(existing_s)]
    if not yck_series.empty:
        db.bulk_upsert(DB_IT, "yck_car_basic_series", yck_series)
        log.info(f"  → 新增车系 {len(yck_series)} 条")
    else:
        log.info("  → 无新车系")

    # ── 颜色回填（DB_ODS 内部） ───────────────────────────────────────────
    log.info("[3/7] 颜色回填（ODS 内部）...")
    n = db.execute(
        DB_ODS,
        """UPDATE config_autohome_detail_info a,
           (SELECT b.model_id, c.color_outside, c.color_outside_code
            FROM config_autohome_major_info_tmp b
            INNER JOIN config_autohome_color c ON b.series_id = c.series_id
            WHERE color_outside IS NOT NULL) b
           SET a.color_outside = b.color_outside, a.color_outside_code = b.color_outside_code
           WHERE a.autohome_id = b.model_id""",
    )
    log.info(f"  → 更新 {n} 条颜色记录")

    # ── 拉取新车型基础信息（yck1）────────────────────────────────────────
    log.info("[4/7] 拉取 ODS 主表 + DB_IT 已有 ID...")
    major = db.query(
        DB_ODS,
        "SELECT model_id, brandid, series_id, status, model_name, model_price, model_year, is_green FROM config_autohome_major_info_tmp WHERE is_check = 1",
    )
    log.info(f"  ODS major: {len(major)} 条")
    brand_it = db.query(DB_IT, "SELECT brandid, Initial, brand_name FROM yck_car_basic_brand")
    series_it = db.query(DB_IT, "SELECT series_id, series_name, series_group_name FROM yck_car_basic_series")
    config_it_ids = db.query(DB_IT, "SELECT autohome_id FROM yck_car_basic_config")

    existing_cfg = _id_set(config_it_ids["autohome_id"])
    log.info(f"  IT 库已有车型 autohome_id: {len(existing_cfg)} 个")
    major = major[~_id_str(major["model_id"]).isin(existing_cfg)]
    log.info(f"  过滤已存在后剩余: {len(major)} 条")

    yck1 = (
        major
        .merge(brand_it, on="brandid", how="inner")
        .merge(series_it, on="series_id", how="inner")
    )
    log.info(f"  merge 品牌+车系后: {len(yck1)} 条")
    yck1 = yck1.rename(columns={
        "model_id": "autohome_id", "brand_name": "brand", "series_name": "series",
        "Initial": "mark", "series_group_name": "factory_name",
        "status": "is_selling", "model_price": "recommend_price", "model_year": "year",
    })
    yck1["type_name"] = (yck1["series"] + " " + yck1["model_name"]).str.strip()
    yck1["produced_place"] = ""

    # ── 拉取详细配置（yck2）──────────────────────────────────────────────
    log.info("[5/7] 拉取详细配置（yck2）...")
    yck2 = db.query(
        DB_ODS,
        """SELECT a.*, '' hl_configs, '' hl_configc, c.car_level
           FROM config_autohome_detail_info a
           INNER JOIN config_autohome_major_info_tmp b ON a.autohome_id = b.model_id
           LEFT JOIN config_autohome_yck_series c ON b.series_id = c.series_id
           WHERE b.is_check = 1""",
    )
    log.info(f"  yck2 原始: {len(yck2)} 条, {len(yck2.columns)} 列")
    yck2 = yck2[~_id_str(yck2["autohome_id"]).isin(existing_cfg)]
    log.info(f"  过滤已存在后: {len(yck2)} 条")

    # car_level 来自 yck_series（SQL 末列），不在 YCK2_RENAME 中，先拆出来
    car_level_series = yck2["car_level"] if "car_level" in yck2.columns else None
    drop_cols = [c for c in ("url", "update_time", "car_level") if c in yck2.columns]
    yck2 = yck2.drop(columns=drop_cols)

    log.info(f"  drop 后列数: {len(yck2.columns)}, YCK2_RENAME 期望: {len(YCK2_RENAME)}")
    if len(yck2.columns) == len(YCK2_RENAME):
        yck2.columns = YCK2_RENAME
    else:
        log.warning(f"  WARNING: 列数不匹配，实际列: {list(yck2.columns)}")
    # level 用 car_level 覆盖（车型等级）
    if car_level_series is not None:
        yck2["level"] = car_level_series.values

    if "emission" in yck2.columns:
        yck2["emission"] = yck2["emission"].astype(str).str.split(" ").str[0]

    yck2 = _split_pair(yck2, _FRONT_COLS, _REAR_COLS)
    yck2 = _normalize_config_flag(yck2)

    # ── 合并 yck1 + yck2 ─────────────────────────────────────────────────
    log.info("[6/7] 合并 yck1 + yck2，写入 DB_IT...")
    yck1["autohome_id"] = yck1["autohome_id"].astype(int)
    yck2["autohome_id"] = yck2["autohome_id"].astype(int)
    yck = yck1.merge(yck2, on="autohome_id", how="inner")
    log.info(f"  合并后: {len(yck)} 条, {len(yck.columns)} 列")

    if yck.empty:
        log.info("  → 无新车型需要同步。")
    else:
        yck = yck.fillna("-")
        for col in ["color_outside", "color_outside_code"]:
            if col in yck.columns:
                yck[col] = yck[col].str.replace("无", "", regex=False)
                yck[col] = yck[col].str.rstrip(";")

        if "environmental_standards_org" in yck.columns:
            yck["environmental_standards"] = (
                yck["environmental_standards_org"].astype(str).apply(_normalize_emission)
            )

        if "type_name" in yck.columns:
            yck["type_name"] = yck["type_name"].str.replace(r" +", " ", regex=True).str.strip()

        # ── 增量去重（不依赖 ODS 基准表，以 DB_IT 为唯一基准）────────────────
        # 1) DataFrame 内部去重：yck1×yck2 merge 后同一 autohome_id 可能多行
        before = len(yck)
        yck = yck.drop_duplicates(subset=["autohome_id"], keep="first")
        if len(yck) < before:
            log.info(f"  DataFrame 内去重: {before} → {len(yck)} 条")

        # 2) 写入前再查一次 IT 库已有 autohome_id（防拉取期间的竞态/重复跑）
        existing_now = _id_set(
            db.query(DB_IT, "SELECT autohome_id FROM yck_car_basic_config")["autohome_id"]
        )
        before = len(yck)
        yck = yck[~_id_str(yck["autohome_id"]).isin(existing_now)]
        if len(yck) < before:
            log.info(f"  二次差集过滤: {before} → {len(yck)} 条")

        if yck.empty:
            log.info("  → 去重后无新车型，跳过写入。")
        else:
            # 只保留 DB_IT 目标表真实存在的列，id 不传由 AUTO_INCREMENT 分配
            target_cols = db.query(DB_IT, "DESCRIBE yck_car_basic_config")["Field"].tolist()
            keep_cols = [c for c in target_cols if c != "id" and c in yck.columns]
            missing = [c for c in target_cols if c != "id" and c not in yck.columns]
            if missing:
                log.warning(f"  注意: 目标表有但 yck 缺失的列(将不写入): {missing}")
            yck = yck[keep_cols]
            log.info(f"  写入列数: {len(keep_cols)}")

            db.bulk_insert(DB_IT, "yck_car_basic_config", yck)
            log.info(f"  → 写入 {len(yck)} 条车型记录")

        # ── 指导价增量更新 ────────────────────────────────────────────────
        log.info("  指导价差异比对...")
        major_prices = db.query(
            DB_ODS,
            "SELECT model_id autohome_id, model_price recommend_price FROM config_autohome_major_info_tmp WHERE is_check = 1",
        )
        # 防御性过滤：排除手动录入车型（>阈值），不参与自动价格比对/更新
        it_prices = db.query(
            DB_IT,
            f"SELECT autohome_id, recommend_price FROM yck_car_basic_config WHERE autohome_id <= {config.MANUAL_AUTOHOME_ID_THRESHOLD}",
        )
        price_diff = major_prices.merge(it_prices, on="autohome_id", suffixes=("_new", "_old"))
        price_diff = price_diff[
            pd.to_numeric(price_diff["recommend_price_new"], errors="coerce") !=
            pd.to_numeric(price_diff["recommend_price_old"], errors="coerce")
        ]
        log.info(f"  → 指导价变更 {len(price_diff)} 条")
        if not price_diff.empty:
            params = [
                (row["recommend_price_new"], int(row["autohome_id"]))
                for _, row in price_diff.iterrows()
            ]
            db.bulk_update(
                DB_IT,
                "UPDATE yck_car_basic_config SET recommend_price=%s WHERE autohome_id=%s",
                params,
            )

    # ── 新能源扩展表增量更新（每次都跑）──────────────────────────────────
    log.info("[7/7] 新能源扩展表增量更新...")
    ev_data = db.query(DB_ODS, "SELECT * FROM config_autohome_ev_info")
    ev_data = ev_data.rename(columns={"model_id": "autohome_id", "ee_fast_charge": "ee_fast_charge(%)"})
    ev_data = ev_data.drop(columns=["add_time", "update_time"], errors="ignore")
    ev_data["ee_fast_charge(%)"] = ev_data["ee_fast_charge(%)"].astype(str).str.replace(r"^%", "-", regex=True)
    for col in ev_data.columns[1:]:
        ev_data[col] = ev_data[col].replace("", "-")

    # 通过 autohome_id 关联取得 IT 库主键 id（无对应 config 记录的 ev 数据丢弃）
    # 防御性过滤：排除手动录入车型（>阈值），其 ev 扩展由人工维护
    basic_config = db.query(
        DB_IT,
        f"SELECT id, autohome_id FROM yck_car_basic_config WHERE autohome_id <= {config.MANUAL_AUTOHOME_ID_THRESHOLD}",
    )
    basic_config["autohome_id"] = _id_str(basic_config["autohome_id"])
    ev_data["autohome_id"] = _id_str(ev_data["autohome_id"])
    ev_data = ev_data.merge(basic_config, on="autohome_id", how="inner")
    ev_data = ev_data[["id"] + [c for c in ev_data.columns if c not in ("id", "autohome_id")]]

    if ev_data.empty:
        log.info("  → 无可关联的新能源记录，跳过。")
    else:
        # REPLACE INTO 按主键 id 增量：新增则插入，已存在则覆盖（参数更新）
        db.bulk_upsert(DB_IT, "yck_car_basic_config_ev", ev_data)
        log.info(f"  → 新能源记录增量写入 {len(ev_data)} 条")

    notifier.send_mail("同步到IT系统的车型库", "同步完成")
    log.info("[autohome_match] 完成。")


if __name__ == "__main__":
    run()
