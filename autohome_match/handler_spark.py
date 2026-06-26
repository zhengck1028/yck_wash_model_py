"""autohome_match 的 PySpark 实现（试点）。

与 autohome_match/handler.py 业务逻辑等价，但：
  - 读取走 Spark JDBC（common.spark.read_sql）
  - 中间计算用 Spark DataFrame API（join / 差集 / 正则清洗）
  - 写回走 common.spark.write_replace / write_insert（临时表 + MERGE）

ODS 内部的 UPDATE（颜色回填）和指导价批量 UPDATE 这类「就地更新」
仍走 common.db 的 SQL（Spark 不适合做这种点更新）。

环境要求见 common/spark.py。试点验证通过后再推广到其他模块。
"""
from __future__ import annotations

from pyspark.sql import functions as F

from common import config, db
from common.logging_setup import get_logger
from common.spark import get_spark, read_sql, write_insert, write_replace

log = get_logger("autohome_match")

DB_ODS = config.DB_ODS
DB_IT = config.DB_IT

_CAR_LEVEL_PATTERN = (
    r"紧凑型SUV|中型SUV|紧凑型车|中大型车|中型车|中大型SUV|大型SUV|大型车|微型车|小型车|MPV|"
    r"跑车|小型SUV|微面|皮卡|微卡|轻客|客车|紧凑型MPV|卡车|中大型MPV|其它|货车|重卡"
)

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


def _normalize_config_flag(sdf, cols: list[str]):
    """对应 pandas 版 _normalize_config_flag：标配→Y / 选配→- / 无/-→N。"""
    for col in [c for c in cols if c in sdf.columns]:
        c = F.col(col).cast("string")
        c = F.regexp_replace(c, r"前标配|后标配|主标配|副标配|标配", "Y")
        c = F.regexp_replace(c, r"前选配|后选配|主选配|副选配|选配", "-")
        c = F.regexp_replace(c, r"前无|后无|主无|副无|无", "N")
        c = F.regexp_replace(c, r"前-|后-|主-|副-|-", "N")
        sdf = sdf.withColumn(col, c)
    return sdf


def _sync_brand() -> None:
    """[1/7] 品牌同步：ODS 有、IT 没有的品牌增量写入 IT。"""
    log.info("[1/7] 品牌同步...")
    yck_brand = read_sql(DB_ODS, "SELECT * FROM config_autohome_yck_brand")
    it_brand = read_sql(DB_IT, "SELECT brandid FROM yck_car_basic_brand")
    # 差集：left_anti join 取 ODS 中 IT 没有的
    new_brand = yck_brand.join(it_brand, on="brandid", how="left_anti")
    n = new_brand.count()
    if n:
        write_replace(DB_IT, "yck_car_basic_brand", new_brand)
        log.info(f"  → 新增品牌 {n} 条")
    else:
        log.info("  → 无新品牌")


def _sync_series() -> None:
    """[2/7] 车系同步。"""
    log.info("[2/7] 车系同步...")
    yck_series = read_sql(
        DB_ODS,
        "SELECT series_id, series_group_name, series_name, brandid, brand_name, car_level, is_import FROM config_autohome_yck_series",
    )
    it_series = read_sql(DB_IT, "SELECT series_id FROM yck_car_basic_series")
    new_series = yck_series.join(it_series, on="series_id", how="left_anti")
    n = new_series.count()
    if n:
        write_replace(DB_IT, "yck_car_basic_series", new_series)
        log.info(f"  → 新增车系 {n} 条")
    else:
        log.info("  → 无新车系")


def _backfill_color() -> None:
    """[3/7] 颜色回填（ODS 内部就地 UPDATE，走 SQL 不走 Spark）。"""
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


def run() -> None:
    """试点入口：先跑通品牌/车系/颜色回填三步，验证 Spark 读写链路。

    车型宽表合并(yck1+yck2)、指导价更新、ev 同步等剩余步骤待环境验证
    通过后再补全（逻辑与 handler.py 对应）。
    """
    get_spark()  # 触发 SparkSession 初始化
    _sync_brand()
    _sync_series()
    _backfill_color()
    log.info("[autohome_match·spark] 试点三步完成。")


if __name__ == "__main__":
    run()
