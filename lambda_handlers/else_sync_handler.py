"""
Lambda 入口：其它数据同步（投诉/经销商报价/销量/口碑/经销商信息/裸车价）

触发 event 示例：
  {}                              # 按当天日期自动决定执行哪些任务
  {"task": "complaint"}           # 强制执行指定任务（手动补跑）

可用 task 值：
  complaint / discount / salesnum / koubei / dealer / owner_price_ah / owner_price_yiche
"""
import traceback

from common import notifier
import else_sync.handler as handler


def lambda_handler(event, context):
    task = event.get("task")
    try:
        handler.run(task=task)
        label = task if task else "auto"
        return {"statusCode": 200, "body": f"else_sync 完成: {label}"}
    except Exception:
        msg = traceback.format_exc()
        notifier.send_mail("其它数据同步异常", msg)
        return {"statusCode": 500, "body": msg}
