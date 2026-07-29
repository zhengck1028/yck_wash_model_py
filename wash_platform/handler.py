"""二手车平台清洗入口（PySpark）。

调度 7 个平台清洗（che168/che58/guazi/rrc/youxin/yiche/ttp），逐个执行、
单平台异常发邮件但不中断其余。全部跑完后刷新车系计数统计表。

对应原 R 流程 main_local/wash_platform/wash_platform.R：
  - 每天执行全部平台清洗
  - 收尾：REPLACE 写 analysis_wide_table_cous_new（按车系×新能源计数）
  - 每周三额外更新宽表车型数据（_update_wide_table_major）

event/参数：
  run()                  → 执行全部平台（含按当天日期决定是否更新宽表车型）
  run(platform="guazi")  → 只执行指定平台（手动补跑）
"""
from __future__ import annotations

import traceback
from datetime import date

from common import config, db, notifier
from common.logging_setup import get_logger
from common.spark import get_spark
from wash_platform.che168_spark import Che168SparkWasher
from wash_platform.che58_spark import Che58SparkWasher
from wash_platform.guazi_spark import GuaziSparkWasher
from wash_platform.rrc_spark import RrcSparkWasher
from wash_platform.ttp_spark import TtpSparkWasher
from wash_platform.yiche_spark import YicheSparkWasher
from wash_platform.youxin_spark import YouxinSparkWasher

log = get_logger("wash_platform")

DB_YUN = config.DB_YUN

# 执行顺序与原 R 流程一致
PLATFORM_MAP = {
    "che168": Che168SparkWasher,
    "che58": Che58SparkWasher,
    "guazi": GuaziSparkWasher,
    "rrc": RrcSparkWasher,
    "youxin": YouxinSparkWasher,
    "yiche": YicheSparkWasher,
    "ttp": TtpSparkWasher,
}

_PLATFORM_NAMES = "'che168','che58','guazi','rrc','youxin','yiche','ttp'"


def _refresh_series_count() -> None:
    """收尾：按车系×新能源刷新计数表 analysis_wide_table_cous_new。

    对应原 R：REPLACE INTO analysis_wide_table_cous_new SELECT ... GROUP BY。
    """
    try:
        db.execute(
            DB_YUN,
            f"""REPLACE INTO analysis_wide_table_cous_new
                SELECT yck_seriesid, is_green, COUNT(*) count_s
                FROM analysis_wide_table_new
                WHERE car_platform IN ({_PLATFORM_NAMES})
                GROUP BY yck_seriesid, is_green""",
        )
        log.info("  车系计数表 analysis_wide_table_cous_new 刷新完成")
    except Exception:
        notifier.send_mail("二手车价格平台清洗异常-analysis_wide_table_new更新失败", traceback.format_exc())


def _update_wide_table_major() -> None:
    """更新分析宽表的车型数据（对应原 R fun_wtb_major_info_upd）。

    车型库 config_vdatabase_yck_model 变更时触发器写入 tri_cvym，这里把
    被 update/delete 的车型字段同步刷新到 analysis_wide_table_new，
    完成后清掉已处理的 tri_cvym 记录。纯 SQL 点更新，不走 Spark。
    """
    upd = db.query(DB_YUN, "SELECT model_id FROM tri_cvym WHERE tri_type IN ('update','delete')")
    if upd.empty:
        log.info("  宽表车型数据：无待更新车型")
        return
    mids = [str(m) for m in upd["model_id"].tolist()]
    cols = [
        "model_year", "yck_brandid", "yck_seriesid", "is_import", "is_green",
        "brand_name brand", "series_name series", "model_name", "liter", "auto",
        "discharge_standard", "car_level", "model_price",
    ]
    set_cols = [c.split(" ")[-1] for c in cols]  # 取别名/列名作为宽表目标列
    skip_ids: list[str] = []
    for mid in mids:
        info = db.query(
            DB_YUN,
            f"SELECT {', '.join(cols)} FROM config_vdatabase_yck_major_info WHERE model_id = %s",
            (mid,),
        )
        if info.empty:
            log.info(f"  无车型数据，跳过：{mid}")
            skip_ids.append(mid)
            continue
        row = info.iloc[0]
        set_clause = ", ".join(f"`{c}`=%s" for c in set_cols)
        params = tuple(row[c] for c in set_cols) + (mid,)
        db.execute(
            DB_YUN,
            f"UPDATE analysis_wide_table_new SET {set_clause} WHERE id_che300 = %s",
            params,
        )
    done_ids = [m for m in mids if m not in skip_ids]
    if done_ids:
        in_clause = ", ".join(done_ids)
        db.execute(DB_YUN, f"DELETE FROM tri_cvym WHERE model_id IN ({in_clause})")
    notifier.send_mail("分析宽表车型数据更新", "更新完成。")
    log.info(f"  宽表车型数据更新完成：{len(done_ids)} 条")


def run(platform: str | None = None) -> None:
    """执行平台清洗。platform 为空则跑全部，否则只跑指定平台。"""
    get_spark()  # 触发 SparkSession 初始化

    targets = [platform] if platform else list(PLATFORM_MAP.keys())
    for name in targets:
        washer_cls = PLATFORM_MAP.get(name)
        if washer_cls is None:
            log.warning(f"未知平台: {name}")
            continue
        try:
            washer_cls().run()
            log.info(f"{name} done.")
        except Exception:
            msg = f"{name} 平台清洗失败:\n{traceback.format_exc()}"
            log.error(msg)
            notifier.send_mail("二手车价格平台清洗异常", msg)

    # 全部平台跑完后刷新计数表（仅全量执行时）
    if platform is None:
        _refresh_series_count()
        # 每周三更新宽表车型数据（对应原 R weekdays==星期三）
        if date.today().weekday() == 2:  # 0=周一, 2=周三
            try:
                _update_wide_table_major()
            except Exception:
                notifier.send_mail("分析宽表车型数据更新异常", traceback.format_exc())

    log.info("[wash_platform] 完成。")


if __name__ == "__main__":
    run()
