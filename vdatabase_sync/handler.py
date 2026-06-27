"""
车型库源数据同步（PySpark 实现）

从本地爬虫库（DB_LOCAL）增量同步到 ODS 层（DB_ODS yck_ods），每周四执行。

同步内容：
  1. 汽车之家新能源参数 config_autohome_ev_info
  2. 汽车之家详细配置 config_autohome_detail_info（含品牌/车系 ID 生成）
  3. 汽车之家主表    config_autohome_major_info_tmp

读取走 Spark JDBC，写回走临时表 MERGE（common.spark.write_replace/write_insert）。
字段清洗（变速箱/排放/排量/新能源参数）用 Spark Column 表达式实现，
规则顺序与原 pandas 版严格一致。
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config, db, notifier
from common.logging_setup import get_logger
from common.spark import get_spark, read_sql, write_insert, write_replace

log = get_logger("vdatabase_sync")

DB_LOCAL = config.DB_LOCAL
DB_ODS = config.DB_ODS

_WEEK_AGO = str(date.today() - timedelta(days=7))

_CAR_LEVEL_PATTERN = (
    r"紧凑型SUV|中型SUV|紧凑型车|中大型车|中型车|中大型SUV|大型SUV|大型车|微型车|小型车|MPV|"
    r"跑车|小型SUV|微面|皮卡|微卡|轻客|客车|紧凑型MPV|卡车|中大型MPV|其它|货车|重卡"
)


# ── 通用字段清洗（Spark Column 表达式，规则顺序对齐 pandas 版）──────────

def _normalize_auto(col: F.Column) -> F.Column:
    """对应 pandas dealFun_auto()，最终只保留 自动双离合/自动/手动/-。"""
    c = col.cast("string")
    c = F.regexp_replace(c, r"CVT无级变速|无级变速|自动.*CVT|CVT.*自动|-CVT|CVT", "自动")
    c = F.regexp_replace(c, r"双离合.*手自动一体", "自动")
    c = F.regexp_replace(c, r"手自一体型|手自动一体型|手自动一体|手自一体|tiptronic", "自动")
    c = F.regexp_replace(c, r"DSG双离合|双离合器|双离合|G-DCT|DCT|DSG", "自动双离合")
    c = F.regexp_replace(c, r"[^a-zA-Z0-9]AT|[-\s]\dAT|AT[\s-]", "自动")
    c = F.regexp_replace(c, r"AT(?![a-zA-Z0-9])", "自动")
    c = F.regexp_replace(c, r"AMT智能手动版|AMT智能手动|AMT|EMT|IMT", "自动")
    c = F.regexp_replace(c, r"(?<![a-zA-Z])MT(?![a-zA-Z])|\dMT|MT[-\s]", "手动")
    c = F.regexp_replace(c, r"(?i)^manual", "手动")
    # 兜底提取：只保留 自动双离合/自动/手动，其余置 -
    ext = F.regexp_extract(c, r"(自动双离合|自动|手动)", 1)
    return F.when(ext == "", "-").otherwise(ext)


def _normalize_discharge(model_name: F.Column, discharge: F.Column) -> F.Column:
    """对应 pandas dealFun_discharge_standard()。"""
    # 从 model_name 提取标准编号
    extracted = F.regexp_extract(
        model_name.cast("string"),
        r"((?:国|欧|京)(?:Ⅱ|Ⅲ|Ⅳ|III|II|IV|VI|Ⅵ|V|二|三|四|五|2|3|4|5)(?:型|)|京(?:5|五|V))",
        1,
    )
    d = discharge.cast("string")
    has_std = d.rlike(r"国|欧|京")
    # 原字段无 国/欧/京 的，用从 model_name 提取的值覆盖
    result = F.when(has_std, d).otherwise(extracted)
    # 仍无 国/欧/京 的置空
    result = F.when(result.rlike(r"国|欧|京"), result).otherwise(F.lit(None))
    # 清洗：化油器→""，数字/罗马→汉字（顺序与 pandas 一致）
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
        result = F.regexp_replace(result, pat, repl)
    return result


def _extract_liter(model_name: F.Column, raw_liter: F.Column) -> F.Column:
    """对应 pandas: str_extract(liter,'[0-9][.][0-9]')，空则从 model_name 再提取。"""
    l1 = F.regexp_extract(raw_liter.cast("string"), r"(\d+\.\d)", 1)
    l2 = F.regexp_extract(model_name.cast("string"), r"(\d+\.\d)", 1)
    return F.when(l1 != "", l1).otherwise(F.when(l2 != "", l2).otherwise(F.lit(None)))


def _clean_model_year(col: F.Column) -> F.Column:
    """对应 pandas: gsub('其它|其他','') + model_year=='0' -> ''。"""
    c = F.regexp_replace(col.cast("string"), r"其它|其他", "")
    return F.when(c == "0", "").otherwise(c)


def _normalize_ev_info(sdf: DataFrame) -> DataFrame:
    """对应 pandas dealFun_ev_info()。"""
    cols = sdf.columns
    if "ee_type" in cols:
        c = F.regexp_replace(F.col("ee_type").cast("string"), r"/| ", "")
        c = F.regexp_replace(c, r"后", "/后")
        sdf = sdf.withColumn("ee_type", c)
    for col in ("ee_fast_charging_time", "ee_slow_charging_time"):
        if col in cols:
            sdf = sdf.withColumn(col, F.regexp_replace(F.col(col).cast("string"), r"标配|选配|待查", "-"))
    if "ee_battery_type" in cols:
        ext = F.regexp_extract(
            F.col("ee_battery_type").cast("string"),
            r"(三元镍钴锰酸锂|磷酸铁锂|三元锂|锂离子|钴酸锂|锰酸锂|镍钴锰|高压锂|镍氢)",
            1,
        )
        sdf = sdf.withColumn("ee_battery_type", F.when(ext != "", F.concat(ext, F.lit("电池"))).otherwise("-"))
    for col in [c for c in cols if c.endswith("range")]:
        c = F.regexp_replace(F.col(col).cast("string"), r"标配|选配", "-")
        c = F.regexp_replace(c, r"km|公里", "")
        sdf = sdf.withColumn(col, c)
    if "ee_battery_quality_assurance" in cols:
        c = F.col("ee_battery_quality_assurance").cast("string")
        for zh, ar in [("三", "3"), ("四", "4"), ("五", "5"), ("六", "6"),
                       ("八", "8"), ("十", "10"), ("待查", "-")]:
            c = F.regexp_replace(c, zh, ar)
        c = F.regexp_replace(c, r"或| ", "")
        sdf = sdf.withColumn("ee_battery_quality_assurance", c)
    if "ee_fast_charge" in cols:
        c = F.col("ee_fast_charge").cast("string")
        sdf = sdf.withColumn("ee_fast_charge", F.when(c != "-", F.concat(c, F.lit("%"))).otherwise(c))
    return sdf


def _fill_blank_dash(sdf: DataFrame, exclude: set[str]) -> DataFrame:
    """非排除列：null/空串 → '-'（对应 pandas fillna('-') + replace('^$','-')）。"""
    for col in sdf.columns:
        if col not in exclude:
            c = F.col(col).cast("string")
            sdf = sdf.withColumn(col, F.when((c.isNull()) | (c == ""), "-").otherwise(c))
    return sdf


# ── 1. 新能源参数同步 ────────────────────────────────────────────────

def _sync_ev(src_table: str) -> None:
    sdf = read_sql(
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
    if len(sdf.head(1)) == 0:
        return
    sdf = _fill_blank_dash(sdf, exclude={"model_id", "add_time", "update_time"})
    sdf = _normalize_ev_info(sdf)
    write_replace(DB_ODS, src_table, sdf)


# ── 2. 汽车之家详细配置同步 ──────────────────────────────────────────

def _sync_detail_autohome() -> None:
    sdf = read_sql(
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
    if len(sdf.head(1)):
        write_replace(DB_ODS, "config_autohome_detail_info", sdf)
    _sync_autohome_brand_series()


def _sync_autohome_brand_series() -> None:
    """品牌/车系 ID 增量同步（从 DB_LOCAL 读，写 DB_ODS）。"""
    # 品牌：DISTINCT 全量，差集补充 DB_ODS 缺失的
    brand_local = read_sql(
        DB_LOCAL,
        "SELECT DISTINCT brand_id brandid, brand_letter Initial, brand_name FROM config_autohome_major_info_tmp",
    )
    if len(brand_local.head(1)):
        try:
            brand_ods = read_sql(DB_ODS, "SELECT brandid FROM config_autohome_yck_brand")
            brand_add = brand_local.join(
                brand_ods.withColumn("brandid", F.col("brandid").cast("string")),
                on=brand_local["brandid"].cast("string") == F.col("brandid").cast("string"),
                how="left_anti",
            ) if False else brand_local.join(brand_ods, on="brandid", how="left_anti")
        except Exception:
            brand_add = brand_local
        if len(brand_add.head(1)):
            brand_add = brand_add.withColumn("car_country", F.lit("无"))
            write_insert(DB_ODS, "config_autohome_yck_brand", brand_add)

    # 车系：含 level、进口处理
    series_local = read_sql(
        DB_LOCAL,
        """SELECT DISTINCT a.series_id, a.series_group_name, a.series_name,
                  a.brand_id brandid, a.brand_name, b.`level` car_level
           FROM config_autohome_major_info_tmp a
           INNER JOIN config_autohome_detail_info b ON a.model_id = b.autohome_id
           WHERE b.`level` != '-'""",
    )
    if len(series_local.head(1)) == 0:
        return
    try:
        series_ods = read_sql(DB_ODS, "SELECT series_id FROM config_autohome_yck_series")
        series_add = series_local.join(series_ods, on="series_id", how="left_anti")
    except Exception:
        series_add = series_local
    if len(series_add.head(1)) == 0:
        return

    # 进口处理（对应 pandas 逻辑）
    import_mask = F.col("series_name").rlike("进口")
    sg = F.when(import_mask, F.concat(F.lit("进口"), F.col("series_group_name"))).otherwise(F.col("series_group_name"))
    series_add = series_add.withColumn("series_group_name", sg)
    series_add = series_add.withColumn(
        "series_group_name", F.regexp_replace(F.col("series_group_name"), r"进口进口进口|进口进口", "进口")
    )
    series_add = series_add.withColumn(
        "series_name", F.upper(F.regexp_replace(F.col("series_name"), r"\(进口\)|\(停售\)", ""))
    )
    series_add = series_add.withColumn(
        "is_import", F.col("series_group_name").rlike("进口").cast("int")
    )
    cl = F.regexp_extract(F.col("car_level"), f"({_CAR_LEVEL_PATTERN})", 1)
    series_add = series_add.withColumn("car_level", F.when(cl != "", cl).otherwise("无"))
    series_add = series_add.select(
        "series_id", "series_group_name", "series_name", "brandid", "brand_name", "car_level", "is_import"
    )
    write_replace(DB_ODS, "config_autohome_yck_series", series_add)


# ── 3. 汽车之家主表同步 ──────────────────────────────────────────────

def _sync_autohome_major() -> None:
    sdf = read_sql(
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
    if len(sdf.head(1)) == 0:
        return
    # 新能源标记：is_green 来自 ev 表 model_id 是否非空
    sdf = sdf.withColumn("is_green_flag", F.when(F.col("is_green").isNotNull(), 1).otherwise(0))
    # fuel 映射：纯电=1 油电=2 插电=3 增程=4
    fuel = F.col("fuel").cast("string")
    fuel_code = (
        F.when(fuel.rlike(".*纯电.*"), "1")
        .when(fuel.rlike(".*油电.*"), "2")
        .when(fuel.rlike(".*插电.*"), "3")
        .when(fuel.rlike(".*增程.*"), "4")
        .otherwise(fuel)
    )
    # is_green：fuel 命中 1-4 则用之，否则用 flag
    sdf = sdf.withColumn(
        "is_green",
        F.when(fuel_code.isin("1", "2", "3", "4"), fuel_code).otherwise(F.col("is_green_flag").cast("string")).cast("int"),
    ).drop("fuel", "is_green_flag")
    # 变速箱
    sdf = sdf.withColumn("auto", _normalize_auto(F.concat(F.col("model_name"), F.col("auto").cast("string"))))
    sdf = sdf.withColumn("auto", F.when(F.col("is_green") == 1, "电动").otherwise(F.col("auto")))
    # 在售状态
    status_ext = F.regexp_extract(F.col("status").cast("string"), r"(停售|在售|即将销售)", 1)
    sdf = sdf.withColumn("status", F.when(status_ext != "", status_ext).otherwise(F.col("status")))
    # 排量
    sdf = sdf.withColumn("liter", _extract_liter(F.col("model_name"), F.col("liter")))
    sdf = sdf.withColumn("liter", F.when(F.col("is_green") == 1, "-").otherwise(F.col("liter")))
    # 排放标准
    sdf = sdf.withColumn("discharge_standard", _normalize_discharge(F.col("model_name"), F.col("discharge_standard")))
    sdf = sdf.withColumn("is_check", F.lit(1))
    write_replace(DB_ODS, "config_autohome_major_info_tmp", sdf)


# ── 主入口 ────────────────────────────────────────────────────────────

def run() -> None:
    get_spark()
    log.info("[vdatabase_sync] 开始...")
    _sync_ev("config_autohome_ev_info")
    _sync_detail_autohome()
    _sync_autohome_major()
    notifier.send_mail("车型库源数据同步完成", f"完成时间：{__import__('datetime').datetime.now()}")
    log.info("[vdatabase_sync] 完成。")


if __name__ == "__main__":
    run()
