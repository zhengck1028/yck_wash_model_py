"""车型库同步 DAG：爬虫库 → ODS → IT 生产库。

数据流:
    NAS 爬虫库(yck-data-center)
        └─ vdatabase_sync ─▶ NAS ODS(yck_ods)
                                  └─ autohome_match ─▶ 云生产库(DB_IT)

调度: 每周四 02:00（与原 R 流程一致），也可在 UI 手动触发/补跑。
依赖: autohome_match 只在 vdatabase_sync 成功后才跑。
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from _common import DEFAULT_ARGS, TZ, on_failure_email


@dag(
    dag_id="vehicle_sync",
    description="车型库同步：爬虫库 → ODS → IT 生产库",
    schedule="0 2 * * 4",  # 每周四 02:00
    start_date=pendulum.datetime(2026, 6, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,  # 同一时刻只跑一个实例，避免增量水位竞态
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure_email},
    tags=["vehicle", "etl"],
)
def vehicle_sync():
    @task
    def vdatabase_sync_task():
        """爬虫库 → ODS：清洗车型主表/详情/新能源参数。"""
        from vdatabase_sync import handler
        handler.run()

    @task
    def autohome_match_task():
        """ODS → IT：增量同步品牌/车系/车型配置/新能源扩展表。"""
        from autohome_match import handler
        handler.run()

    # 依赖链：match 只在 sync 成功后跑
    vdatabase_sync_task() >> autohome_match_task()


vehicle_sync()
