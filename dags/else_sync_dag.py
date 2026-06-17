"""其它数据同步 DAG：投诉/报价/销量/口碑/经销商/车主裸车价。

else_sync.run() 内部已按当天日期自动决定执行哪些子任务
（周四/每月1日/24日/28日各有不同），所以这里每天触发一次，
传 task=None 让它自行判断。手动补跑某个子任务可在 UI 触发并传 conf。

调度: 每天 03:00。
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from _common import DEFAULT_ARGS, TZ, on_failure_email


@dag(
    dag_id="else_sync",
    description="其它数据同步（投诉/报价/销量/口碑/经销商/裸车价）",
    schedule="0 3 * * *",  # 每天 03:00
    start_date=pendulum.datetime(2026, 6, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure_email},
    params={
        # 手动触发时可指定单个子任务补跑；留空则按日期自动判断
        "task": Param("", type="string",
                      description="指定子任务名补跑（complaint/discount/salesnum/koubei/dealer/owner_price_ah/owner_price_yiche），留空=按日期自动"),
    },
    tags=["else", "etl"],
)
def else_sync():
    @task
    def else_sync_task(**context):
        from else_sync import handler
        task_name = (context["params"].get("task") or "").strip() or None
        handler.run(task=task_name)

    else_sync_task()


else_sync()
