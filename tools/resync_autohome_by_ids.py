"""
按指定 autohome_id 重新解析并同步车型数据：爬虫库(DB_LOCAL) → ODS(DB_ODS) → IT 生产库(DB_IT)。

用途：当某些车型在爬虫库被修正/补全后，需要单独重刷到 IT 生产库，
而不必等每周四的全量增量（vehicle_sync DAG）。

与 vehicle_sync DAG 的区别：
  - 取数范围：把 vdatabase_sync / autohome_match 中的「按 add_time/update_time 增量」
    改成「WHERE model_id IN (指定 ID)」。
  - 写入策略：对 DB_IT.yck_car_basic_config 采用「保留 id 的覆盖写入」——
    已存在车型查出原自增主键 id、带 id 用 REPLACE INTO 原地覆盖（id 不变，
    避免其它表通过 id 外键引用时悬空）；全新车型 id 留空由 AUTO_INCREMENT 分配。
    从而支持对已存在车型的强制重刷（覆盖修正）而不破坏主键引用。

清洗逻辑完全复用 vdatabase_sync/handler.py 与 autohome_match/handler.py 的函数，
本脚本只负责「换取数条件 + 换写入策略」，不重新实现任何字段清洗。

用法（在项目根目录执行）：
  # 预览（只读，不写任何库）——强烈建议先跑这个
  python tools/resync_autohome_by_ids.py --ids 12345,67890 --dry-run

  # 从文件读 ID（每行一个，支持 # 注释）
  python tools/resync_autohome_by_ids.py --id-file ids.txt --dry-run

  # 正式写入（先删后插到 DB_IT）
  python tools/resync_autohome_by_ids.py --ids 12345,67890 --commit
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 让 import common / vdatabase_sync / autohome_match 能找到
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from common import config, db
from common.logging_setup import get_logger

# 复用两个 handler 的清洗工具，保证与生产链路逻辑一致
from vdatabase_sync import handler as vsync
from autohome_match import handler as amatch

log = get_logger("resync_autohome_by_ids")

DB_LOCAL = config.DB_LOCAL
DB_ODS = config.DB_ODS
DB_IT = config.DB_IT


# ──────────────────────────────────────────────────────────────────────
# 工具：把 ID 列表拼成安全的 SQL IN 子句
# ──────────────────────────────────────────────────────────────────────

def _in_clause(ids: list[int]) -> str:
    """autohome_id 是整数，直接拼数字串（已在入口校验为 int，无注入风险）。"""
    return ", ".join(str(i) for i in ids)


def _parse_ids(args) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    def _add(token: str, *, strict: bool) -> None:
        """strict=True（命令行 --ids）非整数报错；strict=False（文件）跳过+告警。"""
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

    # 命令行 --ids：手输，非整数视为手误，严格报错
    if args.ids:
        for token in args.ids.replace("\n", ",").split(","):
            _add(token, strict=True)

    # 文件 --id-file：表头/注释/空行/脏行都跳过，不让脚本崩
    if args.id_file:
        lines = Path(args.id_file).read_text(encoding="utf-8-sig").splitlines()
        if args.skip_header and lines:
            log.info(f"  跳过文件首行（表头）: {lines[0].strip()!r}")
            lines = lines[1:]
        for line in lines:
            line = line.split("#", 1)[0].strip()  # 去注释/空白
            if line:
                for token in line.replace(",", " ").split():
                    _add(token, strict=False)

    if not ids:
        raise SystemExit("未提供任何 autohome_id，请用 --ids 或 --id-file 指定。")
    return _exclude_manual(ids)


def _exclude_manual(ids: list[int]) -> list[int]:
    """剔除手动录入区间的 autohome_id（>阈值），避免自动数据覆盖手工维护内容。

    手动录入的车型直接进 IT 生产库、不经爬虫/ODS，本工具一律拒绝重刷它们。
    """
    th = config.MANUAL_AUTOHOME_ID_THRESHOLD
    manual = [i for i in ids if i > th]
    if manual:
        log.warning(
            f"  拒绝 {len(manual)} 个手动录入 id（>{th}），不予重刷: {manual}"
        )
    kept = [i for i in ids if i <= th]
    if not kept:
        raise SystemExit(f"全部 id 都是手动录入区间（>{th}），无可重刷车型。")
    return kept


# ──────────────────────────────────────────────────────────────────────
# 阶段一：爬虫库 → ODS（复用 vdatabase_sync 的清洗函数，仅换 WHERE 条件）
# ──────────────────────────────────────────────────────────────────────

def stage_sync_to_ods(ids: list[int], commit: bool) -> None:
    """把指定车型的 新能源参数 / 详细配置 / 主表 从 DB_LOCAL 清洗后写入 DB_ODS。

    与 vdatabase_sync.run() 对齐，但增量条件改为 model_id IN (...)。
    """
    in_ids = _in_clause(ids)
    log.info(f"[阶段一] 爬虫库 → ODS，目标 {len(ids)} 个 autohome_id")

    # 1) 新能源参数（对应 vsync._sync_ev，按 model_id 取数）─────────────
    ev = db.query(
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
    if not ev.empty:
        for col in ev.columns:
            if col not in ("model_id", "add_time", "update_time"):
                ev[col] = ev[col].fillna("-").astype(str).str.replace(r"^$", "-", regex=True)
        ev = vsync._normalize_ev_info(ev)
        log.info(f"  新能源参数: {len(ev)} 条")
        if commit:
            db.bulk_upsert(DB_ODS, "config_autohome_ev_info", ev)
    else:
        log.info("  新能源参数: 无（非新能源车型属正常）")

    # 2) 详细配置（对应 vsync._sync_detail_autohome 的取数部分）──────────
    detail = db.query(
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
    if not detail.empty:
        log.info(f"  详细配置: {len(detail)} 条")
        if commit:
            db.bulk_upsert(DB_ODS, "config_autohome_detail_info", detail)
    else:
        log.warning("  详细配置: 0 条（这些 ID 可能 level='-' 或 model_year 为空，无法同步！）")

    # 品牌/车系是 DISTINCT 全量差集补充，与具体 ID 无关，直接复用原函数
    if commit:
        vsync._sync_autohome_brand_series()

    # 3) 主表（对应 vsync._sync_autohome_major 的取数 + 清洗）──────────────
    major = db.query(
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
    if major.empty:
        log.warning("  主表: 0 条（ID 不存在 / 即将销售 / 价格为0 / level='-'，无法继续！）")
        return major  # 空，调用方据此判断
    # 以下清洗逻辑与 vsync._sync_autohome_major 完全一致 ────────────────
    major["is_green"] = major["is_green"].notna().astype(int)
    fuel_map = [(r".*纯电.*", "1"), (r".*油电.*", "2"), (r".*插电.*", "3"), (r".*增程.*", "4")]
    for pat, val in fuel_map:
        major.loc[major["fuel"].astype(str).str.match(pat), "fuel"] = val
    major["is_green"] = major.apply(
        lambda r: r["fuel"] if r["fuel"] in ("1", "2", "3", "4") else str(r["is_green"]), axis=1
    ).astype(int)
    major = major.drop(columns=["fuel"])
    major["auto"] = vsync._normalize_auto((major["model_name"] + major["auto"].astype(str)).astype(str))
    major.loc[major["is_green"] == 1, "auto"] = "电动"
    major["status"] = major["status"].astype(str).str.extract(r"(停售|在售|即将销售)")[0]
    major["liter"] = vsync._extract_liter(major["model_name"], major["liter"])
    major.loc[major["is_green"] == 1, "liter"] = "-"
    major["discharge_standard"] = vsync._normalize_discharge(major["model_name"], major["discharge_standard"])
    major["is_check"] = 1
    log.info(f"  主表（清洗后）: {len(major)} 条")
    if commit:
        db.bulk_upsert(DB_ODS, "config_autohome_major_info_tmp", major)
    return major


# ──────────────────────────────────────────────────────────────────────
# 阶段二：ODS → IT 生产库（复用 autohome_match 的清洗，先删后插）
# ──────────────────────────────────────────────────────────────────────

def stage_sync_to_it(ids: list[int], commit: bool) -> None:
    """把指定车型从 DB_ODS 组装成宽表，先删后插到 DB_IT.yck_car_basic_config。

    复用 autohome_match 的 yck1/yck2 组装与字段清洗，但：
      - 取数 WHERE 限定 model_id IN (...)
      - 不做「与 DB_IT 差集」过滤（要重刷已存在车型）
      - 写入前先 DELETE 指定 autohome_id 的旧记录（先删后插）
    """
    in_ids = _in_clause(ids)
    log.info(f"[阶段二] ODS → IT，目标 {len(ids)} 个 autohome_id")

    # 品牌 / 车系：仍按「DB_IT 缺失才补」增量（车系/品牌没有先删后插的必要）
    _sync_brand_series_to_it(commit)

    # 颜色回填（ODS 内部），与原 autohome_match[3/7] 一致，限定本批 ID
    if commit:
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

    # ── yck1：基础信息（限定本批 ID，不做差集过滤）────────────────────
    major = db.query(
        DB_ODS,
        f"""SELECT model_id, brandid, series_id, status, model_name, model_price,
                   model_year, is_green
            FROM config_autohome_major_info_tmp
            WHERE is_check = 1 AND model_id IN ({in_ids})""",
    )
    if major.empty:
        log.warning("  ODS 主表无这些 ID（阶段一可能未写入或被过滤），跳过 IT 写入。")
        return
    brand_it = db.query(DB_IT, "SELECT brandid, Initial, brand_name FROM yck_car_basic_brand")
    series_it = db.query(DB_IT, "SELECT series_id, series_name, series_group_name FROM yck_car_basic_series")

    yck1 = major.merge(brand_it, on="brandid", how="inner").merge(series_it, on="series_id", how="inner")
    yck1 = yck1.rename(columns={
        "model_id": "autohome_id", "brand_name": "brand", "series_name": "series",
        "Initial": "mark", "series_group_name": "factory_name",
        "status": "is_selling", "model_price": "recommend_price", "model_year": "year",
    })
    yck1["type_name"] = (yck1["series"] + " " + yck1["model_name"]).str.strip()
    yck1["produced_place"] = ""

    # ── yck2：详细配置（限定本批 ID）──────────────────────────────────
    yck2 = db.query(
        DB_ODS,
        f"""SELECT a.*, '' hl_configs, '' hl_configc, c.car_level
            FROM config_autohome_detail_info a
            INNER JOIN config_autohome_major_info_tmp b ON a.autohome_id = b.model_id
            LEFT JOIN config_autohome_yck_series c ON b.series_id = c.series_id
            WHERE b.is_check = 1 AND a.autohome_id IN ({in_ids})""",
    )
    if yck2.empty:
        log.warning("  ODS 详细配置无这些 ID，跳过 IT 写入。")
        return

    car_level_series = yck2["car_level"] if "car_level" in yck2.columns else None
    drop_cols = [c for c in ("url", "update_time", "car_level") if c in yck2.columns]
    yck2 = yck2.drop(columns=drop_cols)
    if len(yck2.columns) == len(amatch.YCK2_RENAME):
        yck2.columns = amatch.YCK2_RENAME
    else:
        log.warning(f"  列数不匹配：实际 {len(yck2.columns)} vs 期望 {len(amatch.YCK2_RENAME)}")
    if car_level_series is not None:
        yck2["level"] = car_level_series.values
    if "emission" in yck2.columns:
        yck2["emission"] = yck2["emission"].astype(str).str.split(" ").str[0]
    yck2 = amatch._split_pair(yck2, amatch._FRONT_COLS, amatch._REAR_COLS)
    yck2 = amatch._normalize_config_flag(yck2)

    # ── 合并 yck1 + yck2 ──────────────────────────────────────────────
    yck1["autohome_id"] = yck1["autohome_id"].astype(int)
    yck2["autohome_id"] = yck2["autohome_id"].astype(int)
    yck = yck1.merge(yck2, on="autohome_id", how="inner")
    yck = yck.drop_duplicates(subset=["autohome_id"], keep="first")
    log.info(f"  组装宽表: {len(yck)} 条")
    if yck.empty:
        log.warning("  宽表为空（品牌/车系未匹配上？），跳过。")
        return

    yck = yck.fillna("-")
    for col in ("color_outside", "color_outside_code"):
        if col in yck.columns:
            yck[col] = yck[col].str.replace("无", "", regex=False).str.rstrip(";")
    if "environmental_standards_org" in yck.columns:
        yck["environmental_standards"] = (
            yck["environmental_standards_org"].astype(str).apply(amatch._normalize_emission)
        )
    if "type_name" in yck.columns:
        yck["type_name"] = yck["type_name"].str.replace(r" +", " ", regex=True).str.strip()

    # 只保留目标表真实存在的列
    target_cols = db.query(DB_IT, "DESCRIBE yck_car_basic_config")["Field"].tolist()
    keep_cols = [c for c in target_cols if c != "id" and c in yck.columns]
    final = yck[["autohome_id"] + [c for c in keep_cols if c != "autohome_id"]] \
        if "autohome_id" in keep_cols else yck[keep_cols].copy()

    # 实际写入的 autohome_id
    final_ids = sorted(int(x) for x in final["autohome_id"].tolist()) if "autohome_id" in final.columns else ids

    # ── 回填已存在车型的原主键 id，保证覆盖写入不改 id ─────────────────
    # 已存在 → 带上原 id，用 REPLACE INTO 按主键覆盖（id 不变，外键不悬空）；
    # 全新   → id 留空(None)，由 AUTO_INCREMENT 分配。
    existing_ids = db.query(
        DB_IT,
        f"SELECT id, autohome_id FROM yck_car_basic_config WHERE autohome_id IN ({_in_clause(final_ids)})",
    )
    id_map = {}
    if not existing_ids.empty:
        id_map = dict(zip(amatch._id_str(existing_ids["autohome_id"]), existing_ids["id"]))
    final = final.copy()
    final["id"] = amatch._id_str(final["autohome_id"]).map(id_map)  # 命中=原id，未命中=NaN
    n_reuse = int(final["id"].notna().sum())
    n_new = len(final) - n_reuse
    write_cols = ["id"] + keep_cols

    log.info(f"  最终待写入: {len(final)} 条（保留原 id 覆盖 {n_reuse} 条，新增 {n_new} 条），{len(keep_cols)} 业务列")
    log.info(f"  覆盖 autohome_id: {final_ids}")

    if not commit:
        # 带时间戳避免覆盖历史预览；文件被占用（IDE/Excel 打开）也不让脚本崩
        from datetime import datetime
        out = Path(__file__).parent / f"resync_preview_config_{datetime.now():%Y%m%d_%H%M%S}.csv"
        try:
            final[write_cols].to_csv(out, index=False, encoding="utf-8-sig")
            log.info(f"  [DRY-RUN] 未写库。预览已存: {out}")
        except PermissionError:
            log.warning(f"  [DRY-RUN] 预览 CSV 写入失败（文件被占用？）：{out}，跳过落盘。")
        return

    # ── 保留 id 的覆盖写入：REPLACE INTO 按主键 id 覆盖 ────────────────
    # bulk_upsert 用 REPLACE INTO；带 id 的行原地覆盖（id 不变），
    # id 为 None 的新行触发 AUTO_INCREMENT。
    db.bulk_upsert(DB_IT, "yck_car_basic_config", final[write_cols])
    log.info(f"  REPLACE 写入: {len(final)} 条（{n_reuse} 条保留原 id）")

    # ── 新能源扩展表：同样保留 id 覆盖（按 IT 主键 id 关联）────────────
    _resync_ev_to_it(final_ids, commit)


def _sync_brand_series_to_it(commit: bool) -> None:
    """品牌/车系增量补充到 DB_IT（缺失才插），与 autohome_match[1-2/7] 一致。"""
    yck_brand = db.query(DB_ODS, "SELECT * FROM config_autohome_yck_brand")
    if not yck_brand.empty:
        existing = set(db.query(DB_IT, "SELECT brandid FROM yck_car_basic_brand")["brandid"].astype(str))
        yck_brand = yck_brand[~yck_brand["brandid"].astype(str).isin(existing)]
    if not yck_brand.empty:
        log.info(f"  新增品牌 {len(yck_brand)} 条")
        if commit:
            db.bulk_upsert(DB_IT, "yck_car_basic_brand", yck_brand)

    yck_series = db.query(
        DB_ODS,
        "SELECT series_id, series_group_name, series_name, brandid, brand_name, car_level, is_import FROM config_autohome_yck_series",
    )
    if not yck_series.empty:
        existing_s = set(db.query(DB_IT, "SELECT series_id FROM yck_car_basic_series")["series_id"].astype(str))
        yck_series = yck_series[~yck_series["series_id"].astype(str).isin(existing_s)]
    if not yck_series.empty:
        log.info(f"  新增车系 {len(yck_series)} 条")
        if commit:
            db.bulk_upsert(DB_IT, "yck_car_basic_series", yck_series)


def _resync_ev_to_it(autohome_ids: list[int], commit: bool) -> None:
    """新能源扩展表按主键 id 覆盖（对应 autohome_match[7/7]，限定本批 ID）。

    config_ev 的主键 id 与 yck_car_basic_config.id 一一对应，故用 REPLACE INTO
    带 id 覆盖——id 不变，外键不悬空。
    """
    in_ids = _in_clause(autohome_ids)
    ev_data = db.query(DB_ODS, f"SELECT * FROM config_autohome_ev_info WHERE model_id IN ({in_ids})")
    if ev_data.empty:
        log.info("  新能源扩展表: 本批无新能源记录，跳过。")
        return
    ev_data = ev_data.rename(columns={"model_id": "autohome_id", "ee_fast_charge": "ee_fast_charge(%)"})
    ev_data = ev_data.drop(columns=["add_time", "update_time"], errors="ignore")
    ev_data["ee_fast_charge(%)"] = ev_data["ee_fast_charge(%)"].astype(str).str.replace(r"^%", "-", regex=True)
    for col in ev_data.columns[1:]:
        ev_data[col] = ev_data[col].replace("", "-")

    # 关联 IT 库主键 id（config 覆盖写入后即可取到，已存在车型沿用原 id）
    basic_config = db.query(DB_IT, f"SELECT id, autohome_id FROM yck_car_basic_config WHERE autohome_id IN ({in_ids})")
    if basic_config.empty:
        log.info("  新能源扩展表: config 中无对应记录，跳过。")
        return
    basic_config["autohome_id"] = amatch._id_str(basic_config["autohome_id"])
    ev_data["autohome_id"] = amatch._id_str(ev_data["autohome_id"])
    ev_data = ev_data.merge(basic_config, on="autohome_id", how="inner")
    ev_data = ev_data[["id"] + [c for c in ev_data.columns if c not in ("id", "autohome_id")]]
    if ev_data.empty:
        log.info("  新能源扩展表: 关联后为空，跳过。")
        return
    if commit:
        # REPLACE INTO 按主键 id 覆盖（id 不变），无需 DELETE
        db.bulk_upsert(DB_IT, "yck_car_basic_config_ev", ev_data)
    log.info(f"  新能源扩展表: 覆盖写入 {len(ev_data)} 条")


# ──────────────────────────────────────────────────────────────────────
# 入口
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="按 autohome_id 重新解析并同步车型：爬虫库 → ODS → IT 生产库（先删后插）。",
    )
    ap.add_argument("--ids", help="逗号分隔的 autohome_id，如 12345,67890")
    ap.add_argument("--id-file", help="ID 文件路径，每行一个（支持 # 注释；非整数行自动跳过）")
    ap.add_argument("--skip-header", action="store_true", help="跳过 --id-file 的第一行（表头）")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="只读预览，不写任何库（默认）")
    g.add_argument("--commit", action="store_true", help="正式写入（阶段一写 ODS，阶段二先删后插 IT）")
    args = ap.parse_args()

    commit = bool(args.commit)
    ids = _parse_ids(args)

    mode = "COMMIT（会写库）" if commit else "DRY-RUN（只读）"
    log.info("=" * 60)
    log.info(f"按 ID 重新同步  |  模式: {mode}  |  {len(ids)} 个 autohome_id")
    log.info(f"ID: {ids}")
    log.info("=" * 60)

    stage_sync_to_ods(ids, commit)
    stage_sync_to_it(ids, commit)

    if commit:
        log.info("完成：已写入 ODS 并先删后插到 IT 生产库。")
    else:
        log.info("DRY-RUN 完成：未改动任何库。确认无误后加 --commit 正式执行。")


if __name__ == "__main__":
    main()
