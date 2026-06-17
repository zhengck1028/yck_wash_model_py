"""
Lambda 入口：平台 ID 与 YCK 标准 ID 匹配

触发 event 示例：
  {}    # 无需参数，匹配所有平台
"""
from plat_id_match import handler


def lambda_handler(event, context):
    handler.run()
    return {"statusCode": 200, "body": "plat_id_match 完成"}
