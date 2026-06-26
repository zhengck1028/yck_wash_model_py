"""autohome_match 的 PySpark 实现（试点）。

与 autohome_match/handler.py 业务逻辑等价，但：
  - 读取走 Spark JDBC（common.spark.read_sql）
  - 中间计算用 Spark DataFrame API（join / 差集 / 正则清洗）
  - 写回走 common.spark.write_replace / write_insert（临时表 + MERGE）

ODS 内部 UPDATE（颜色回填）、指导价点更新这类「就地更新」仍走
common.db 的 SQL（Spark 不适合做这种点更新）。

环境要求见 common/spark.py。
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config, db
from common.logging_setup import get_logger
from common.spark import get_spark, read_sql, write_insert, write_replace

log = get_logger("autohome_match")

DB_ODS = config.DB_ODS
DB_IT = config.DB_IT

# 排放标准规范化映射（顺序敏感，与 pandas 版 _EMISSION_MAP 一致）
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

# yck2 列重命名（与 pandas 版 YCK2_RENAME 一致，按位置对齐）
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


# ── 清洗工具（Spark Column 表达式，对齐 pandas 版）──────────────────────

def _normalize_emission_col(col: F.Column) -> F.Column:
    """对应 pandas _normalize_emission：顺序敏感的链式替换。"""
    c = col.cast("string")
    for pattern, repl in _EMISSION_MAP:
        c = F.regexp_replace(c, pattern, repl)
    return c


def _split_pair(sdf: DataFrame) -> DataFrame:
    """对应 pandas _split_pair：前字段去 /后部分，后字段去 前/部分。"""
    for fc, rc in zip(_FRONT_COLS, _REAR_COLS):
        if fc in sdf.columns:
            sdf = sdf.withColumn(fc, F.regexp_replace(F.col(fc).cast("string"), r"\?\\/.*|\\/.*", ""))
        if rc in sdf.columns:
            sdf = sdf.withColumn(rc, F.regexp_replace(F.col(rc).cast("string"), r".*\\/\\?|.*\\/", ""))
    return sdf


def _normalize_config_flag(sdf: DataFrame) -> DataFrame:
    """对应 pandas _normalize_config_flag：标配→Y / 选配→- / 无/-→N。"""
    for col in [c for c in _CONFIG_COLS if c in sdf.columns]:
        c = F.col(col).cast("string")
        c = F.regexp_replace(c, r"前标配|后标配|主标配|副标配|标配", "Y")
        c = F.regexp_replace(c, r"前选配|后选配|主选配|副选配|选配", "-")
        c = F.regexp_replace(c, r"前无|后无|主无|副无|无", "N")
        c = F.regexp_replace(c, r"前-|后-|主-|副-|-", "N")
        sdf = sdf.withColumn(col, c)
    return sdf


def _id_norm(col: F.Column) -> F.Column:
    """规范化 ID 为干净整数字符串（对应 pandas _id_str/_id_set）。"""
    return F.col(col).cast("long").cast("string") if isinstance(col, str) else col


# ── 各步骤 ─────────────────────────────────────────────────────────────

def _sync_brand() -> None:
    """[1/7] 品牌同步。"""
    log.info("[1/7] 品牌同步...")
    yck = read_sql(DB_ODS, "SELECT * FROM config_autohome_yck_brand")
    it = read_sql(DB_IT, "SELECT brandid FROM yck_car_basic_brand")
    new = yck.join(it, on="brandid", how="left_anti")
    n = new.count()
    if n:
        write_replace(DB_IT, "yck_car_basic_brand", new)
        log.info(f"  → 新增品牌 {n} 条")
    else:
        log.info("  → 无新品牌")


def _sync_series() -> None:
    """[2/7] 车系同步。"""
    log.info("[2/7] 车系同步...")
    yck = read_sql(
        DB_ODS,
        "SELECT series_id, series_group_name, series_name, brandid, brand_name, car_level, is_import FROM config_autohome_yck_series",
    )
    it = read_sql(DB_IT, "SELECT series_id FROM yck_car_basic_series")
    new = yck.join(it, on="series_id", how="left_anti")
    n = new.count()
    if n:
        write_replace(DB_IT, "yck_car_basic_series", new)
        log.info(f"  → 新增车系 {n} 条")
    else:
        log.info("  → 无新车系")


def _backfill_color() -> None:
    """[3/7] 颜色回填（ODS 内部就地 UPDATE，走 SQL）。"""
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


def _sync_config() -> None:
    """[4-6/7] 车型宽表合并（yck1+yck2）并写入 IT。"""
    # ── yck1：基础信息 ──
    log.info("[4/7] 拉取 ODS 主表 + DB_IT 已有 ID...")
    major = read_sql(
        DB_ODS,
        "SELECT model_id, brandid, series_id, status, model_name, model_price, model_year, is_green FROM config_autohome_major_info_tmp WHERE is_check = 1",
    )
    brand_it = read_sql(DB_IT, "SELECT brandid, Initial, brand_name FROM yck_car_basic_brand")
    series_it = read_sql(DB_IT, "SELECT series_id, series_name, series_group_name FROM yck_car_basic_series")
    cfg_ids = read_sql(DB_IT, "SELECT autohome_id FROM yck_car_basic_config").withColumn(
        "autohome_id", F.col("autohome_id").cast("long")
    )

    # 差集：过滤掉 IT 已有的（model_id 对 autohome_id）
    major = major.withColumn("model_id", F.col("model_id").cast("long"))
    major = major.join(
        cfg_ids.withColumnRenamed("autohome_id", "model_id"), on="model_id", how="left_anti"
    )

    yck1 = (
        major.join(brand_it, on="brandid", how="inner")
        .join(series_it, on="series_id", how="inner")
        .withColumnRenamed("model_id", "autohome_id")
        .withColumnRenamed("brand_name", "brand")
        .withColumnRenamed("series_name", "series")
        .withColumnRenamed("Initial", "mark")
        .withColumnRenamed("series_group_name", "factory_name")
        .withColumnRenamed("status", "is_selling")
        .withColumnRenamed("model_price", "recommend_price")
        .withColumnRenamed("model_year", "year")
    )
    yck1 = yck1.withColumn("type_name", F.trim(F.concat_ws(" ", F.col("series"), F.col("model_name"))))
    yck1 = yck1.withColumn("produced_place", F.lit(""))

    # ── yck2：详细配置 ──
    log.info("[5/7] 拉取详细配置（yck2）...")
    yck2 = read_sql(
        DB_ODS,
        """SELECT a.*, '' hl_configs, '' hl_configc, c.car_level
           FROM config_autohome_detail_info a
           INNER JOIN config_autohome_major_info_tmp b ON a.autohome_id = b.model_id
           LEFT JOIN config_autohome_yck_series c ON b.series_id = c.series_id
           WHERE b.is_check = 1""",
    )
    yck2 = yck2.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    yck2 = yck2.join(
        cfg_ids.withColumnRenamed("autohome_id", "autohome_id"), on="autohome_id", how="left_anti"
    )

    # car_level 单独留存，drop 掉 url/update_time/car_level 后按位置重命名
    has_car_level = "car_level" in yck2.columns
    if has_car_level:
        car_level_df = yck2.select("autohome_id", "car_level")
    drop_cols = [c for c in ("url", "update_time", "car_level") if c in yck2.columns]
    yck2 = yck2.drop(*drop_cols)

    if len(yck2.columns) == len(YCK2_RENAME):
        yck2 = yck2.toDF(*YCK2_RENAME)
    else:
        log.warning(f"  WARNING: 列数不匹配 {len(yck2.columns)} vs {len(YCK2_RENAME)}: {yck2.columns}")

    # level 用 car_level 覆盖
    if has_car_level:
        yck2 = yck2.drop("level").join(
            car_level_df.withColumnRenamed("car_level", "level"), on="autohome_id", how="left"
        )

    # emission 取空格前第一段
    if "emission" in yck2.columns:
        yck2 = yck2.withColumn("emission", F.split(F.col("emission").cast("string"), " ").getItem(0))

    yck2 = _split_pair(yck2)
    yck2 = _normalize_config_flag(yck2)

    # ── 合并 yck1 + yck2 ──
    log.info("[6/7] 合并 yck1 + yck2，写入 DB_IT...")
    yck = yck1.join(yck2, on="autohome_id", how="inner")

    # 颜色清洗
    for col in ("color_outside", "color_outside_code"):
        if col in yck.columns:
            c = F.regexp_replace(F.col(col).cast("string"), "无", "")
            c = F.regexp_replace(c, ";+$", "")  # rstrip(';')
            yck = yck.withColumn(col, c)
    # 排放标准中文化
    if "environmental_standards_org" in yck.columns:
        yck = yck.withColumn("environmental_standards", _normalize_emission_col(F.col("environmental_standards_org")))
    # type_name 多空格归一
    if "type_name" in yck.columns:
        yck = yck.withColumn("type_name", F.trim(F.regexp_replace(F.col("type_name"), r" +", " ")))

    # DataFrame 内去重（同 autohome_id 取首行）
    from pyspark.sql import Window
    w = Window.partitionBy("autohome_id").orderBy(F.lit(1))
    yck = yck.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # 二次差集：再查一次 IT 已有 autohome_id
    existing_now = read_sql(DB_IT, "SELECT autohome_id FROM yck_car_basic_config").withColumn(
        "autohome_id", F.col("autohome_id").cast("long")
    )
    yck = yck.join(existing_now, on="autohome_id", how="left_anti")

    if len(yck.head(1)) == 0:
        log.info("  → 去重后无新车型，跳过写入。")
        return

    # 只保留目标表真实存在的列（id 由 AUTO_INCREMENT，不写）
    target_cols = db.query(DB_IT, "DESCRIBE yck_car_basic_config")["Field"].tolist()
    keep_cols = [c for c in target_cols if c != "id" and c in yck.columns]
    # 缺失列补 "-"（fillna 等价：写入前对 keep_cols 填充）
    yck = yck.select(*keep_cols).fillna("-")
    n = yck.count()
    write_insert(DB_IT, "yck_car_basic_config", yck)
    log.info(f"  → 写入 {n} 条车型记录")


def _update_prices() -> None:
    """[6/7 尾] 指导价增量更新（点更新走 SQL bulk_update）。"""
    log.info("  指导价差异比对...")
    major_prices = db.query(
        DB_ODS,
        "SELECT model_id autohome_id, model_price recommend_price FROM config_autohome_major_info_tmp WHERE is_check = 1",
    )
    it_prices = db.query(DB_IT, "SELECT autohome_id, recommend_price FROM yck_car_basic_config")
    import pandas as pd
    diff = major_prices.merge(it_prices, on="autohome_id", suffixes=("_new", "_old"))
    diff = diff[
        pd.to_numeric(diff["recommend_price_new"], errors="coerce")
        != pd.to_numeric(diff["recommend_price_old"], errors="coerce")
    ]
    log.info(f"  → 指导价变更 {len(diff)} 条")
    if not diff.empty:
        params = [(r["recommend_price_new"], int(r["autohome_id"])) for _, r in diff.iterrows()]
        db.bulk_update(
            DB_IT, "UPDATE yck_car_basic_config SET recommend_price=%s WHERE autohome_id=%s", params
        )


def _sync_ev() -> None:
    """[7/7] 新能源扩展表增量更新（按 IT 主键 id 用 REPLACE 语义覆盖）。"""
    log.info("[7/7] 新能源扩展表增量更新...")
    ev = read_sql(DB_ODS, "SELECT * FROM config_autohome_ev_info")
    ev = ev.withColumnRenamed("model_id", "autohome_id").withColumnRenamed("ee_fast_charge", "ee_fast_charge(%)")
    for c in ("add_time", "update_time"):
        if c in ev.columns:
            ev = ev.drop(c)
    # ee_fast_charge(%)：开头 % 替成 -
    ev = ev.withColumn("ee_fast_charge(%)", F.regexp_replace(F.col("ee_fast_charge(%)").cast("string"), r"^%", "-"))
    # 空串 → -（除 autohome_id 外）
    for c in ev.columns:
        if c != "autohome_id":
            ev = ev.withColumn(c, F.when(F.col(c) == "", "-").otherwise(F.col(c)))

    # 关联 IT 主键 id
    basic = read_sql(DB_IT, "SELECT id, autohome_id FROM yck_car_basic_config")
    basic = basic.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    ev = ev.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    ev = ev.join(basic, on="autohome_id", how="inner")
    # 排列为 id 在首、去掉 autohome_id
    cols = ["id"] + [c for c in ev.columns if c not in ("id", "autohome_id")]
    ev = ev.select(*cols)

    if len(ev.head(1)) == 0:
        log.info("  → 无可关联的新能源记录，跳过。")
        return
    write_replace(DB_IT, "yck_car_basic_config_ev", ev)
    log.info("  → 新能源记录增量写入完成")


def run() -> None:
    get_spark()  # 触发 SparkSession 初始化
    _sync_brand()
    _sync_series()
    _backfill_color()
    _sync_config()
    _update_prices()
    _sync_ev()
    from common import notifier
    notifier.send_mail("同步到IT系统的车型库", "同步完成（Spark）")
    log.info("[autohome_match·spark] 完成。")


if __name__ == "__main__":
    run()
