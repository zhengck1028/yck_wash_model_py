"""
Lambda 入口：本地爬虫库 → 阿里云 DB_YUN 车型数据同步

触发 event 示例：
  {}      # 执行全量同步（所有平台主表 + 详情表 + 新能源表）
"""
import traceback

from common import notifier
import vdatabase_sync.handler as handler


def lambda_handler(event, context):
    try:
        handler.run()
        return {"statusCode": 200, "body": "vdatabase_sync 完成"}
    except Exception:
        msg = traceback.format_exc()
        notifier.send_mail("爬虫车型库同步异常", msg)
        return {"statusCode": 500, "body": msg}
