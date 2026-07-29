"""
按指定 autohome_id 重新解析并同步车型数据：爬虫库(DB_LOCAL) → ODS(DB_ODS) → IT 生产库(DB_IT)。

用途：当某些车型在爬虫库被修正/补全后，需要单独重刷到 IT 生产库，
而不必等每周四的全量增量（vehicle_sync DAG）。

与 vehicle_sync DAG 的区别：
  - 取数范围：把 vdatabase_sync / autohome_match 中的「按 add_time/update_time 增量」
    改成「WHERE model_id IN (指定 ID)」。
  - 写入策略：对 DB_IT.yck_car_basic_config 用 REPLACE 语义（临时表 MERGE），
    组装宽表时带上 IT 库现有主键 id，已存在车型原地覆盖（id 不变，避免其它
    表通过 id 外键引用悬空），全新车型 id 留空由 AUTO_INCREMENT 分配。

清洗逻辑复用 vdatabase_sync/handler.py 与 autohome_match/handler.py 的 Spark 函数，
本脚本只负责「换取数条件 + 换写入策略」，不重新实现字段清洗。全程 PySpark。

用法（在项目根目录执行）：
  # 预览（只读，不写任何库）——强烈建议先跑这个
  python tools/resync_autohome_by_ids.py --ids 12345,67890 --dry-run

  # 从文件读 ID（每行一个，支持 # 注释；--skip-header 跳表头）
  python tools/resync_autohome_by_ids.py --id-file tools/ids.csv --skip-header --dry-run

  # 把业务日志落盘（Spark 噪音不进文件，便于查看结果）
  python tools/resync_autohome_by_ids.py --ids 12345 --dry-run --out-file resync.log

  # 把最终待写入 IT 的宽表导出 CSV 供核对（不写库）
  python tools/resync_autohome_by_ids.py --id-file tools/ids.txt --dry-run --dump-csv tools/preview.csv

  # 正式写入（REPLACE 覆盖到 DB_IT）
  python tools/resync_autohome_by_ids.py --ids 12345,67890 --commit
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 让 import common / vdatabase_sync / autohome_match 能找到
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from common import config, db
from common.logging_setup import get_logger
from common.spark import get_spark, read_sql, write_replace

# 复用两个 Spark handler 的清洗函数，保证与生产链路逻辑一致
from vdatabase_sync import handler as vsync
from autohome_match import handler as amatch

log = get_logger("resync_autohome_by_ids")

DB_LOCAL = config.DB_LOCAL
DB_ODS = config.DB_ODS
DB_IT = config.DB_IT


# ──────────────────────────────────────────────────────────────────────
# ID 解析
# ──────────────────────────────────────────────────────────────────────

def _in_clause(ids: list[int]) -> str:
    """autohome_id 是整数，直接拼数字串（已在入口校验为 int，无注入风险）。"""
    return ", ".join(str(i) for i in ids)


def _parse_ids(args) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    def _add(token: str, *, strict: bool) -> None:
        token = token.strip()
        if not token:
            return
        try:
            v = int(token)
        except ValueError:
            if strict:
                raise SystemExit(f"非法 autohome_id（不是整数）: {token!r}")
            log.warning(f"  跳过非整数行: {token!r}")
            return
        if v not in seen:
            seen.add(v)
            ids.append(v)

    if args.ids:
        for token in args.ids.replace("\n", ",").split(","):
            _add(token, strict=True)

    if args.id_file:
        lines = Path(args.id_file).read_text(encoding="utf-8-sig").splitlines()
        if args.skip_header and lines:
            log.info(f"  跳过文件首行（表头）: {lines[0].strip()!r}")
            lines = lines[1:]
        for line in lines:
            line = line.split("#", 1)[0].strip()
            if line:
                for token in line.replace(",", " ").split():
                    _add(token, strict=False)

    if not ids:
        raise SystemExit("未提供任何 autohome_id，请用 --ids 或 --id-file 指定。")
    return _exclude_manual(ids)


def _exclude_manual(ids: list[int]) -> list[int]:
    """剔除手动录入区间的 autohome_id（>阈值），避免自动数据覆盖手工维护内容。"""
    th = config.MANUAL_AUTOHOME_ID_THRESHOLD
    manual = [i for i in ids if i > th]
    if manual:
        log.warning(f"  拒绝 {len(manual)} 个手动录入 id（>{th}），不予重刷: {manual}")
    kept = [i for i in ids if i <= th]
    if not kept:
        raise SystemExit(f"全部 id 都是手动录入区间（>{th}），无可重刷车型。")
    return kept


# ──────────────────────────────────────────────────────────────────────
# 阶段一：爬虫库 → ODS（Spark，仅换 WHERE 条件为 model_id IN）
# ──────────────────────────────────────────────────────────────────────

def stage_sync_to_ods(ids: list[int], commit: bool) -> None:
    in_ids = _in_clause(ids)
    log.info(f"[阶段一] 爬虫库 → ODS，目标 {len(ids)} 个 autohome_id")

    # 1) 新能源参数（对应 vsync._sync_ev）
    ev = read_sql(
        DB_LOCAL,
        f"""SELECT model_id, ee_type, ee_total_power, ee_total_torque,
                   ee_front_max_kw, ee_front_max_Nm, ee_after_max_kw, ee_after_max_Nm,
                   ee_system_integrated_kw, ee_system_integrated_Nm,
                   ee_driving_number, ee_layout, ee_battery_type,
                   ee_pure_electric_range, ee_battery_capacity, ee_power_consumption,
                   ee_battery_quality_assurance, ee_fast_charging_time,
                   ee_slow_charging_time, ee_fast_charge,
                   add_time, add_time AS update_time
            FROM config_autohome_ev_info
            WHERE model_id IN ({in_ids})""",
    )
    if len(ev.head(1)):
        ev = vsync._fill_blank_dash(ev, exclude={"model_id", "add_time", "update_time"})
        ev = vsync._normalize_ev_info(ev)
        log.info(f"  新能源参数: {ev.count()} 条")
        if commit:
            write_replace(DB_ODS, "config_autohome_ev_info", ev)
    else:
        log.info("  新能源参数: 无（非新能源车型属正常）")

    # 2) 详细配置（对应 vsync._sync_detail_autohome 取数）
    detail = read_sql(
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
              AND b.autohome_id IN ({in_ids})""",
    )
    if len(detail.head(1)):
        log.info(f"  详细配置: {detail.count()} 条")
        if commit:
            write_replace(DB_ODS, "config_autohome_detail_info", detail)
    else:
        log.warning("  详细配置: 0 条（这些 ID 可能 level='-' 或 model_year 为空，无法同步！）")

    # 品牌/车系 DISTINCT 全量差集补充，与具体 ID 无关，复用原函数
    if commit:
        vsync._sync_autohome_brand_series()

    # 3) 主表（对应 vsync._sync_autohome_major 取数 + 清洗）
    major = read_sql(
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
              AND a.model_id IN ({in_ids})""",
    )
    if len(major.head(1)) == 0:
        log.warning("  主表: 0 条（ID 不存在 / 即将销售 / 价格为0 / level='-'，无法继续！）")
        return
    # 清洗逻辑与 vsync._sync_autohome_major 一致（Spark 表达式）
    major = major.withColumn("is_green_flag", F.when(F.col("is_green").isNotNull(), 1).otherwise(0))
    fuel = F.col("fuel").cast("string")
    fuel_code = (
        F.when(fuel.rlike(".*纯电.*"), "1")
        .when(fuel.rlike(".*油电.*"), "2")
        .when(fuel.rlike(".*插电.*"), "3")
        .when(fuel.rlike(".*增程.*"), "4")
        .otherwise(fuel)
    )
    major = major.withColumn(
        "is_green",
        F.when(fuel_code.isin("1", "2", "3", "4"), fuel_code).otherwise(F.col("is_green_flag").cast("string")).cast("int"),
    ).drop("fuel", "is_green_flag")
    major = major.withColumn("auto", vsync._normalize_auto(F.concat(F.col("model_name"), F.col("auto").cast("string"))))
    major = major.withColumn("auto", F.when(F.col("is_green") == 1, "电动").otherwise(F.col("auto")))
    status_ext = F.regexp_extract(F.col("status").cast("string"), r"(停售|在售|即将销售)", 1)
    major = major.withColumn("status", F.when(status_ext != "", status_ext).otherwise(F.col("status")))
    major = major.withColumn("liter", vsync._extract_liter(F.col("model_name"), F.col("liter")))
    major = major.withColumn("liter", F.when(F.col("is_green") == 1, "-").otherwise(F.col("liter")))
    major = major.withColumn("discharge_standard", vsync._normalize_discharge(F.col("model_name"), F.col("discharge_standard")))
    major = major.withColumn("is_check", F.lit(1))
    log.info(f"  主表（清洗后）: {major.count()} 条")
    if commit:
        write_replace(DB_ODS, "config_autohome_major_info_tmp", major)


# ──────────────────────────────────────────────────────────────────────
# 阶段二：ODS → IT 生产库（Spark，限定 model_id IN，REPLACE 保留 id 覆盖）
# ──────────────────────────────────────────────────────────────────────

def stage_sync_to_it(ids: list[int], commit: bool, dump_csv: str | None = None) -> None:
    in_ids = _in_clause(ids)
    log.info(f"[阶段二] ODS → IT，目标 {len(ids)} 个 autohome_id")

    # 品牌/车系增量补充（缺失才插）
    if commit:
        amatch._sync_brand()
        amatch._sync_series()
        # 颜色回填，限定本批 ID
        n = db.execute(
            DB_ODS,
            f"""UPDATE config_autohome_detail_info a,
               (SELECT b.model_id, c.color_outside, c.color_outside_code
                FROM config_autohome_major_info_tmp b
                INNER JOIN config_autohome_color c ON b.series_id = c.series_id
                WHERE color_outside IS NOT NULL) b
               SET a.color_outside = b.color_outside, a.color_outside_code = b.color_outside_code
               WHERE a.autohome_id = b.model_id AND a.autohome_id IN ({in_ids})""",
        )
        log.info(f"  颜色回填: 更新 {n} 条")

    # ── yck1：基础信息（限定本批 ID，不做差集过滤——要重刷已存在车型）──
    major = read_sql(
        DB_ODS,
        f"""SELECT model_id, brandid, series_id, status, model_name, model_price,
                   model_year, is_green
            FROM config_autohome_major_info_tmp
            WHERE is_check = 1 AND model_id IN ({in_ids})""",
    )
    if len(major.head(1)) == 0:
        log.warning("  ODS 主表无这些 ID（阶段一可能未写入或被过滤），跳过 IT 写入。")
        return
    brand_it = read_sql(DB_IT, "SELECT brandid, Initial, brand_name FROM yck_car_basic_brand")
    series_it = read_sql(DB_IT, "SELECT series_id, series_name, series_group_name FROM yck_car_basic_series")

    major = major.withColumn("model_id", F.col("model_id").cast("long"))
    yck1 = (
        major.join(brand_it, on="brandid", how="inner").join(series_it, on="series_id", how="inner")
        .withColumnRenamed("model_id", "autohome_id").withColumnRenamed("brand_name", "brand")
        .withColumnRenamed("series_name", "series").withColumnRenamed("Initial", "mark")
        .withColumnRenamed("series_group_name", "factory_name").withColumnRenamed("status", "is_selling")
        .withColumnRenamed("model_price", "recommend_price").withColumnRenamed("model_year", "year")
    )
    yck1 = yck1.withColumn("type_name", F.trim(F.concat_ws(" ", F.col("series"), F.col("model_name"))))
    yck1 = yck1.withColumn("produced_place", F.lit(""))

    # ── yck2：详细配置（限定本批 ID）──
    yck2 = read_sql(
        DB_ODS,
        f"""SELECT a.*, '' hl_configs, '' hl_configc, c.car_level
            FROM config_autohome_detail_info a
            INNER JOIN config_autohome_major_info_tmp b ON a.autohome_id = b.model_id
            LEFT JOIN config_autohome_yck_series c ON b.series_id = c.series_id
            WHERE b.is_check = 1 AND a.autohome_id IN ({in_ids})""",
    )
    if len(yck2.head(1)) == 0:
        log.warning("  ODS 详细配置无这些 ID，跳过 IT 写入。")
        return
    yck2 = yck2.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    has_cl = "car_level" in yck2.columns
    if has_cl:
        car_level_df = yck2.select("autohome_id", "car_level")
    yck2 = yck2.drop(*[c for c in ("url", "update_time", "car_level") if c in yck2.columns])
    if len(yck2.columns) == len(amatch.YCK2_RENAME):
        yck2 = yck2.toDF(*amatch.YCK2_RENAME)
    else:
        log.warning(f"  列数不匹配：实际 {len(yck2.columns)} vs 期望 {len(amatch.YCK2_RENAME)}")
    if has_cl:
        yck2 = yck2.drop("level").join(car_level_df.withColumnRenamed("car_level", "level"), on="autohome_id", how="left")
    if "emission" in yck2.columns:
        yck2 = yck2.withColumn("emission", F.split(F.col("emission").cast("string"), " ").getItem(0))
    yck2 = amatch._split_pair(yck2)
    yck2 = amatch._normalize_config_flag(yck2)

    # ── 合并 yck1 + yck2 ──
    yck = yck1.join(yck2, on="autohome_id", how="inner")
    # DataFrame 内去重（同 autohome_id 取首行）
    from pyspark.sql import Window
    w = Window.partitionBy("autohome_id").orderBy(F.lit(1))
    yck = yck.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    for col in ("color_outside", "color_outside_code"):
        if col in yck.columns:
            c = F.regexp_replace(F.col(col).cast("string"), "无", "")
            yck = yck.withColumn(col, F.regexp_replace(c, ";+$", ""))
    if "environmental_standards_org" in yck.columns:
        yck = yck.withColumn("environmental_standards", amatch._normalize_emission_col(F.col("environmental_standards_org")))
    if "type_name" in yck.columns:
        yck = yck.withColumn("type_name", F.trim(F.regexp_replace(F.col("type_name"), r" +", " ")))

    # ── 保留 id 覆盖：带上 IT 库现有主键 id，REPLACE 原地覆盖 ──
    target_cols = db.query(DB_IT, "DESCRIBE yck_car_basic_config")["Field"].tolist()
    keep_cols = [c for c in target_cols if c != "id" and c in yck.columns]
    existing = read_sql(DB_IT, f"SELECT id, autohome_id FROM yck_car_basic_config WHERE autohome_id IN ({in_ids})")
    existing = existing.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    yck = yck.join(existing, on="autohome_id", how="left")  # 命中带 id，未命中 id=null
    final = yck.select("id", *keep_cols).fillna("-", subset=keep_cols)

    # 转 pandas 做写入前数据验证（剔除错位/异常行）
    from common.validation import validate_car_config

    pdf = final.toPandas()
    n_before = len(pdf)
    n_reuse_before = int(pdf["id"].notna().sum()) if "id" in pdf.columns else 0
    log.info(f"  组装完成: {n_before} 条（保留原 id 覆盖 {n_reuse_before} 条，新增 {n_before - n_reuse_before} 条）")

    pdf, rep = validate_car_config(pdf)
    rep.log(log)

    # 导出校验后（干净）的待写入宽表到本地 CSV（文件被占用不让脚本崩）
    if dump_csv and len(pdf):
        try:
            pdf.to_csv(dump_csv, index=False, encoding="utf-8-sig")
            log.info(f"  已导出待写入宽表: {dump_csv}（{len(pdf)} 行，已剔除 ERROR）")
        except PermissionError:
            log.warning(f"  导出 CSV 失败（文件被占用？）: {dump_csv}，跳过落盘。")

    if len(pdf) == 0:
        log.warning("  校验后无有效数据，跳过写入。")
        return

    if not commit:
        log.info("  [DRY-RUN] 未写库。")
        return

    # 校验后的 pandas 转回 Spark 写入
    final_clean = get_spark().createDataFrame(pdf)
    write_replace(DB_IT, "yck_car_basic_config", final_clean)
    log.info(f"  REPLACE 写入: {len(pdf)} 条")

    # 新能源扩展表：按 IT 主键 id REPLACE 覆盖
    _resync_ev_to_it(ids, commit)


def _resync_ev_to_it(autohome_ids: list[int], commit: bool) -> None:
    """新能源扩展表按主键 id 覆盖（对应 amatch 新能源同步，限定本批 ID）。"""
    in_ids = _in_clause(autohome_ids)
    ev = read_sql(DB_ODS, f"SELECT * FROM config_autohome_ev_info WHERE model_id IN ({in_ids})")
    if len(ev.head(1)) == 0:
        log.info("  新能源扩展表: 本批无新能源记录，跳过。")
        return
    ev = ev.withColumnRenamed("model_id", "autohome_id").withColumnRenamed("ee_fast_charge", "ee_fast_charge(%)")
    for c in ("add_time", "update_time"):
        if c in ev.columns:
            ev = ev.drop(c)
    ev = ev.withColumn("ee_fast_charge(%)", F.regexp_replace(F.col("ee_fast_charge(%)").cast("string"), r"^%", "-"))
    for c in ev.columns:
        if c != "autohome_id":
            ev = ev.withColumn(c, F.when(F.col(c) == "", "-").otherwise(F.col(c)))

    basic = read_sql(DB_IT, f"SELECT id, autohome_id FROM yck_car_basic_config WHERE autohome_id IN ({in_ids})")
    basic = basic.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    ev = ev.withColumn("autohome_id", F.col("autohome_id").cast("long"))
    ev = ev.join(basic, on="autohome_id", how="inner")
    ev = ev.select("id", *[c for c in ev.columns if c not in ("id", "autohome_id")])
    if len(ev.head(1)) == 0:
        log.info("  新能源扩展表: 关联后为空，跳过。")
        return
    if commit:
        write_replace(DB_IT, "yck_car_basic_config_ev", ev)
    log.info(f"  新能源扩展表: 覆盖写入 {ev.count()} 条")


# ──────────────────────────────────────────────────────────────────────
# 入口
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="按 autohome_id 重新解析并同步车型：爬虫库 → ODS → IT 生产库（Spark，REPLACE 保留 id 覆盖）。",
    )
    ap.add_argument("--ids", help="逗号分隔的 autohome_id，如 12345,67890")
    ap.add_argument("--id-file", help="ID 文件路径，每行一个（支持 # 注释；非整数行自动跳过）")
    ap.add_argument("--skip-header", action="store_true", help="跳过 --id-file 的第一行（表头）")
    ap.add_argument("--out-file", help="把业务日志同时写到该文件（Spark 噪音不进文件）")
    ap.add_argument("--dump-csv", help="把最终待写入 IT 的宽表导出到该 CSV（供核对，dry-run/commit 都可）")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="只读预览，不写任何库（默认）")
    g.add_argument("--commit", action="store_true", help="正式写入（阶段一写 ODS，阶段二 REPLACE 覆盖 IT）")
    args = ap.parse_args()

    # 可选：把业务日志落盘（只有本脚本 logger 的输出进文件，Spark stderr 不进）
    if args.out_file:
        import logging
        fh = logging.FileHandler(args.out_file, mode="w", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
        log.addHandler(fh)
        log.info(f"日志同时写入: {args.out_file}")

    commit = bool(args.commit)
    ids = _parse_ids(args)

    mode = "COMMIT（会写库）" if commit else "DRY-RUN（只读）"
    log.info("=" * 60)
    log.info(f"按 ID 重新同步  |  模式: {mode}  |  {len(ids)} 个 autohome_id")
    log.info(f"ID: {ids}")
    log.info("=" * 60)

    get_spark()
    stage_sync_to_ods(ids, commit)
    stage_sync_to_it(ids, commit, dump_csv=args.dump_csv)

    if commit:
        log.info("完成：已写入 ODS 并 REPLACE 覆盖到 IT 生产库。")
    else:
        log.info("DRY-RUN 完成：未改动任何库。确认无误后加 --commit 正式执行。")


if __name__ == "__main__":
    main()
