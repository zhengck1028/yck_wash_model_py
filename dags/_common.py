"""DAG 共享配置与工具。

所有 DAG 复用这里的默认参数、时区、失败告警回调，
保证重试/超时/告警行为一致。
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

# 项目时区（cron 表达式按此解释）
TZ = pendulum.timezone("Asia/Shanghai")

# DAG task 默认参数：重试 2 次、退避、单 task 超时熔断
DEFAULT_ARGS = {
    "owner": "yck-etl",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
}


def on_failure_email(context) -> None:
    """task 失败回调：复用项目现有邮件 notifier 发告警。

    Airflow 自身也会记录失败，这里额外发邮件保持与原 R/Lambda 流程一致。
    """
    try:
        from common import notifier

        ti = context.get("task_instance")
        dag_id = context.get("dag").dag_id if context.get("dag") else "?"
        task_id = ti.task_id if ti else "?"
        exec_date = context.get("logical_date") or context.get("execution_date")
        exception = context.get("exception")
        log_url = ti.log_url if ti else ""

        body = (
            f"<b>DAG:</b> {dag_id}<br>"
            f"<b>Task:</b> {task_id}<br>"
            f"<b>执行时间:</b> {exec_date}<br>"
            f"<b>异常:</b> {exception}<br>"
            f"<b>日志:</b> <a href='{log_url}'>{log_url}</a>"
        )
        notifier.send_mail(f"[Airflow] {dag_id}.{task_id} 执行失败", body)
    except Exception as e:  # 告警本身失败不能影响主流程
        print(f"[on_failure_email] 告警发送失败: {e}")
