"""
预览脚本：模拟 autohome_match 的完整逻辑，但只输出 CSV，不写任何数据库。

数据来源：
  - config_autohome_*  读 DB_LOCAL（本地爬虫 ETL 后的数据）
  - yck_car_basic_*    读 DB_IT（存在性检查，差集判断）

输出文件（保存在当前目录）：
  preview_brand.csv        新品牌（DB_IT 里还没有的）
  preview_series.csv       新车系
  preview_config.csv       新车型宽表（合并后）
  preview_price_update.csv 指导价有变化的车型

用法（在项目根目录执行）：
  python tools/preview_autohome_match.py
"""
from __future__ import annotations

import re
import sys
import os
from datetime import date
from pathlib import Path

# 让 import common/autohome_match 能找到
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from common import config, db

DB_LOCAL = config.DB_LOCAL
DB_IT = config.DB_IT

OUT_DIR = Path(__file__).parent

# ── 复用 autohome_match/handler.py 的常量和工具函数 ──────────────────

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


def _normalize_emission(s: str) -> str:
    for pattern, repl in _EMISSION_MAP:
        s = re.sub(pattern, repl, s)
    return s


def _split_pair(df: pd.DataFrame) -> pd.DataFrame:
    for fc, rc in zip(_FRONT_COLS, _REAR_COLS):
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


# ── 主逻辑（只读，不写库）────────────────────────────────────────────

def preview() -> None:
    print("=" * 60)
    print("autohome_match 预览（DB_LOCAL → 对比 DB_IT，不写库）")
    print("=" * 60)

    # ── 1. 新品牌 ──
    brand_local = db.query(
        DB_LOCAL,
        "SELECT DISTINCT brandid, Initial, brand_name FROM config_autohome_yck_brand",
    )
    brand_it = db.query(DB_IT, "SELECT brandid FROM yck_car_basic_brand")
    existing_brand_ids = set(brand_it["brandid"].astype(str))
    yck_brand = brand_local[~brand_local["brandid"].astype(str).isin(existing_brand_ids)]
    print(f"\n[品牌] 本地共 {len(brand_local)} 条，DB_IT 已有 {len(brand_it)} 条，新增 {len(yck_brand)} 条")
    if not yck_brand.empty:
        print(yck_brand.to_string(index=False))
        yck_brand.to_csv(OUT_DIR / "preview_brand.csv", index=False, encoding="utf-8-sig")
        print(f"  → 已保存 preview_brand.csv")

    # ── 2. 新车系 ──
    series_local = db.query(
        DB_LOCAL,
        """SELECT series_id, series_group_name, series_name, brandid, brand_name, car_level, is_import
           FROM config_autohome_yck_series""",
    )
    series_it = db.query(DB_IT, "SELECT series_id FROM yck_car_basic_series")
    existing_series_ids = set(series_it["series_id"].astype(str))
    yck_series = series_local[~series_local["series_id"].astype(str).isin(existing_series_ids)]
    print(f"\n[车系] 本地共 {len(series_local)} 条，DB_IT 已有 {len(series_it)} 条，新增 {len(yck_series)} 条")
    if not yck_series.empty:
        print(yck_series.head(20).to_string(index=False))
        yck_series.to_csv(OUT_DIR / "preview_series.csv", index=False, encoding="utf-8-sig")
        print(f"  → 已保存 preview_series.csv（共 {len(yck_series)} 行）")

    # ── 3. 新车型宽表 ──
    # yck1：基础信息
    yck1 = db.query(
        DB_LOCAL,
        """SELECT a.model_id autohome_id, b.Initial mark, b.brandid, b.brand_name brand,
                  c.series_id, c.series_name series, `status` is_selling,
                  CONCAT(c.series_name,' ',model_name) type_name,
                  model_price recommend_price, model_year year,
                  c.series_group_name factory_name, '' produced_place, a.is_green
           FROM config_autohome_major_info_tmp a
           INNER JOIN yck_car_basic_brand b ON a.brandid = b.brandid
           INNER JOIN yck_car_basic_series c ON a.series_id = c.series_id
           LEFT JOIN yck_car_basic_config d ON a.model_id = d.autohome_id
           WHERE d.autohome_id IS NULL AND a.is_check = 1""",
    )
    # yck1 的 JOIN 对象 yck_car_basic_brand/series 需要从 DB_IT 读
    # 用两步实现：先从 DB_LOCAL 拉 major_info，再 join DB_IT 的 brand/series
    brand_it_full = db.query(DB_IT, "SELECT brandid, Initial, brand_name FROM yck_car_basic_brand")
    series_it_full = db.query(DB_IT, "SELECT series_id, series_name, series_group_name FROM yck_car_basic_series")
    config_it_ids = db.query(DB_IT, "SELECT autohome_id FROM yck_car_basic_config")

    major = db.query(
        DB_LOCAL,
        "SELECT model_id, brandid, series_id, status, model_name, model_price, model_year, is_green FROM config_autohome_major_info_tmp WHERE is_check = 1",
    )
    existing_config_ids = set(config_it_ids["autohome_id"].astype(str))
    major = major[~major["model_id"].astype(str).isin(existing_config_ids)]

    yck1 = (
        major
        .merge(brand_it_full, on="brandid", how="inner")
        .merge(series_it_full, on="series_id", how="inner")
    )
    yck1 = yck1.rename(columns={"model_id": "autohome_id", "brand_name": "brand", "series_name": "series"})
    yck1["type_name"] = (yck1["series"] + " " + yck1["model_name"]).str.strip()
    yck1["mark"] = yck1["Initial"]
    yck1["factory_name"] = yck1["series_group_name"]
    yck1["produced_place"] = ""
    yck1["is_selling"] = yck1["status"]
    yck1["recommend_price"] = yck1["model_price"]
    yck1["year"] = yck1["model_year"]

    # yck2：详细配置
    yck2 = db.query(
        DB_LOCAL,
        """SELECT b.*, '' hl_configs, '' hl_configc
           FROM config_autohome_major_info_tmp a
           INNER JOIN config_autohome_detail_info b ON a.model_id = b.autohome_id
           WHERE a.is_check = 1""",
    )
    if "url" in yck2.columns:
        yck2 = yck2.drop(columns=["url"])
    if len(yck2.columns) == len(YCK2_RENAME):
        yck2.columns = YCK2_RENAME
    elif len(yck2.columns) == len(YCK2_RENAME) + 1:
        # car_level 可能多出一列
        yck2 = yck2.iloc[:, :len(YCK2_RENAME)]
        yck2.columns = YCK2_RENAME

    if "emission" in yck2.columns:
        yck2["emission"] = yck2["emission"].astype(str).str.split(" ").str[0]

    yck2 = _split_pair(yck2)
    yck2 = _normalize_config_flag(yck2)

    # 合并
    yck1["autohome_id"] = yck1["autohome_id"].astype(int)
    yck2["autohome_id"] = yck2["autohome_id"].astype(int)
    yck = yck1.merge(yck2, on="autohome_id", how="inner")

    print(f"\n[车型] 新增（差集）共 {len(yck)} 条")
    if not yck.empty:
        # 排放标准中文化
        if "environmental_standards_org" in yck.columns:
            yck["environmental_standards"] = (
                yck["environmental_standards_org"].astype(str).apply(_normalize_emission)
            )
        # 颜色清理
        for col in ["color_outside", "color_outside_code"]:
            if col in yck.columns:
                yck[col] = yck[col].astype(str).str.replace("无", "", regex=False).str.rstrip(";")
        # type_name 整理
        if "type_name" in yck.columns:
            yck["type_name"] = yck["type_name"].str.replace(r" +", " ", regex=True).str.strip()

        preview_cols = ["autohome_id", "brand", "series", "type_name", "year", "recommend_price", "is_green"]
        print(yck[[c for c in preview_cols if c in yck.columns]].head(30).to_string(index=False))
        yck.to_csv(OUT_DIR / "preview_config.csv", index=False, encoding="utf-8-sig")
        print(f"  → 已保存 preview_config.csv（共 {len(yck)} 行，{len(yck.columns)} 列）")

    # ── 4. 指导价变更 ──
    major_all = db.query(
        DB_LOCAL,
        "SELECT model_id autohome_id, model_price recommend_price FROM config_autohome_major_info_tmp WHERE is_check = 1",
    )
    config_it_prices = db.query(DB_IT, "SELECT autohome_id, recommend_price FROM yck_car_basic_config")
    merged_prices = major_all.merge(config_it_prices, on="autohome_id", suffixes=("_new", "_old"))
    merged_prices["recommend_price_new"] = pd.to_numeric(merged_prices["recommend_price_new"], errors="coerce")
    merged_prices["recommend_price_old"] = pd.to_numeric(merged_prices["recommend_price_old"], errors="coerce")
    price_changes = merged_prices[
        merged_prices["recommend_price_new"] != merged_prices["recommend_price_old"]
    ]
    print(f"\n[指导价] 有变化的共 {len(price_changes)} 条")
    if not price_changes.empty:
        print(price_changes.head(20).to_string(index=False))
        price_changes.to_csv(OUT_DIR / "preview_price_update.csv", index=False, encoding="utf-8-sig")
        print(f"  → 已保存 preview_price_update.csv")

    print("\n" + "=" * 60)
    print("预览完成，请核查以上数据后再决定是否执行正式写入。")
    print("=" * 60)


if __name__ == "__main__":
    preview()
