"""
Lambda 入口：汽车之家车型库同步

触发 event 示例：
  {}    # 无需参数
"""
from autohome_match import handler


def lambda_handler(event, context):
    handler.run()
    return {"statusCode": 200, "body": "autohome_match 完成"}
