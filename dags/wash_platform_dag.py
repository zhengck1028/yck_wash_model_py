"""二手车平台清洗 DAG。

调度 7 个平台（che168/che58/guazi/rrc/youxin/yiche/ttp）的在售车源清洗，
清洗结果写入分析宽表 analysis_wide_table_new，并刷新车系计数表。

wash_platform.handler.run() 内部逐平台执行、单平台失败发邮件不中断其余。
手动补跑单个平台可在 UI 触发并传 conf {"platform": "guazi"}。

调度: 每天 04:00（与车型库同步 vehicle_sync 02:00 / else_sync 03:00 错开）。
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from _common import DEFAULT_ARGS, TZ, on_failure_email


@dag(
    dag_id="wash_platform",
    description="二手车平台清洗（che168/che58/guazi/rrc/youxin/yiche/ttp）",
    schedule="0 4 * * *",  # 每天 04:00
    start_date=pendulum.datetime(2026, 6, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure_email},
    params={
        "platform": Param("", type="string",
                          description="指定单平台补跑（che168/che58/guazi/rrc/youxin/yiche/ttp），留空=全部"),
    },
    tags=["wash", "platform", "etl"],
)
def wash_platform():
    @task
    def wash_platform_task(**context):
        from wash_platform import handler
        platform = (context["params"].get("platform") or "").strip() or None
        handler.run(platform=platform)

    wash_platform_task()


wash_platform()
