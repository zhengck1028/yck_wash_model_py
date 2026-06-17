"""
Lambda 入口：YCK 车型库增量维护（品牌/车系/车型 ID 自增）

触发 event 示例：
  {}    # 无需参数
"""
from vdatabase import handler


def lambda_handler(event, context):
    handler.run()
    return {"statusCode": 200, "body": "vdatabase 完成"}
