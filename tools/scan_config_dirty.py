"""扫描 IT 生产库 yck_car_basic_config 全表，用 common.validation 规则
找出错位/脏数据的 autohome_id。只读，不写任何库。

默认只扫自动录入行（autohome_id <= 阈值）；手动录入行（>阈值）人工维护，
格式可能不同，不参与扫描。

输出：
  tools/dirty_ids.csv        —— 错位 id 清单（autohome_id, id, 命中字段数, 首要原因）
  tools/dirty_detail.csv     —— 逐字段错位明细
  终端 + tools/scan_report.log —— 汇总

用法：
  python tools/scan_config_dirty.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from common import config, db
from common.logging_setup import get_logger
from common.validation import validate_car_config

log = get_logger("scan_config_dirty")
OUT_DIR = Path(__file__).parent

TH = config.MANUAL_AUTOHOME_ID_THRESHOLD
BATCH = 5000


def main() -> None:
    # 全表列（与校验器字段名对齐）
    total = db.query(
        config.DB_IT, f"SELECT COUNT(*) c FROM yck_car_basic_config WHERE autohome_id <= {TH}"
    )["c"].iloc[0]
    log.info(f"扫描自动录入行: {int(total)} 条（分批 {BATCH}）")

    all_err_rows = []      # 每条错位记录：autohome_id, id, n_err, first_reason
    all_detail = []        # 逐字段：autohome_id, 字段, 原因

    offset = 0
    scanned = 0
    dirty_ids: set = set()
    while True:
        df = db.query(
            config.DB_IT,
            f"SELECT * FROM yck_car_basic_config WHERE autohome_id <= {TH} "
            f"ORDER BY id LIMIT {BATCH} OFFSET {offset}",
        )
        if df.empty:
            break
        scanned += len(df)
        _, rep = validate_car_config(df)

        # 口径：只有 ERROR（铁定错位）算脏数据。WARN（Y/Y 未拆分、商用车尺寸
        # 等合法边缘值）仅记录到明细供参考，不进脏清单。
        by_id: dict = {}
        for _, aid, col, why in rep.errors:
            by_id.setdefault(aid, []).append((col, why))
            all_detail.append({"autohome_id": aid, "level": "ERROR", "field": col, "reason": why})
        for _, aid, col, why in rep.warns:  # WARN 只记明细，不算脏
            all_detail.append({"autohome_id": aid, "level": "WARN", "field": col, "reason": why})
        for aid, items in by_id.items():
            dirty_ids.add(aid)
            first = items[0]
            all_err_rows.append({
                "autohome_id": aid,
                "n_error_fields": len(items),
                "first_reason": f"[{first[0]}] {first[1]}",
            })

        log.info(f"  进度 {scanned}/{int(total)}，累计脏 id {len(dirty_ids)}")
        offset += BATCH

    # id → 主键 id 补充（便于定位）
    err_df = pd.DataFrame(all_err_rows)
    if not err_df.empty:
        ids_str = ", ".join(str(i) for i in err_df["autohome_id"].tolist())
        pk = db.query(config.DB_IT, f"SELECT id, autohome_id, brand, series FROM yck_car_basic_config WHERE autohome_id IN ({ids_str})")
        pk["autohome_id"] = pk["autohome_id"].astype(str)
        err_df["autohome_id"] = err_df["autohome_id"].astype(str)
        err_df = err_df.merge(pk, on="autohome_id", how="left")
        err_df = err_df[["autohome_id", "id", "brand", "series", "n_error_fields", "first_reason"]]
        err_df = err_df.sort_values("n_error_fields", ascending=False)

        def _safe_csv(df_out, name):
            """文件被占用（Excel/IDE 打开）时换带时间戳名，避免脚本崩。"""
            from datetime import datetime
            path = OUT_DIR / f"{name}.csv"
            try:
                df_out.to_csv(path, index=False, encoding="utf-8-sig")
                return path
            except PermissionError:
                alt = OUT_DIR / f"{name}_{datetime.now():%H%M%S}.csv"
                df_out.to_csv(alt, index=False, encoding="utf-8-sig")
                log.warning(f"  {name}.csv 被占用，改存 {alt.name}")
                return alt

        _safe_csv(err_df, "dirty_ids")
        _safe_csv(pd.DataFrame(all_detail), "dirty_detail")

    n_err_total = sum(1 for r in all_detail if r["level"] == "ERROR")
    n_warn_total = len(all_detail) - n_err_total
    log.info("=" * 60)
    log.info(f"扫描完成: 共 {scanned} 行，检出脏数据 {len(dirty_ids)} 条（口径：含 ERROR 真错位；WARN 仅记明细不算脏）")
    log.info(f"脏数据占比: {len(dirty_ids) / scanned * 100:.2f}%")
    log.info(f"字段问题合计: {len(all_detail)} 处（其中 ERROR {n_err_total} 计入脏数据；WARN {n_warn_total} 仅参考）")
    log.info(f"清单已存: tools/dirty_ids.csv（{len(dirty_ids)} 条）")
    log.info(f"明细已存: tools/dirty_detail.csv")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
