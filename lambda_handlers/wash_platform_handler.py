"""
Lambda 入口：二手车平台数据清洗

触发 event 示例：
  {"platform": "guazi"}        # 单平台
  {"platform": "all"}          # 所有平台
"""
import traceback

from common import notifier
from wash_platform.ttp import TtpWasher
from wash_platform.guazi import GuaziWasher
from wash_platform.rrc import RrcWasher
from wash_platform.che58 import Che58Washer
from wash_platform.che168 import Che168Washer
from wash_platform.yiche import YicheWasher
from wash_platform.youxin import YouxinWasher

PLATFORM_MAP = {
    "ttp": TtpWasher,
    "guazi": GuaziWasher,
    "rrc": RrcWasher,
    "che58": Che58Washer,
    "che168": Che168Washer,
    "yiche": YicheWasher,
    "youxin": YouxinWasher,
}


def lambda_handler(event, context):
    platform = event.get("platform", "all")
    targets = list(PLATFORM_MAP.keys()) if platform == "all" else [platform]

    errors = []
    for name in targets:
        if name not in PLATFORM_MAP:
            errors.append(f"未知平台: {name}")
            continue
        try:
            PLATFORM_MAP[name]().run()
        except Exception as e:
            msg = f"{name} 平台清洗失败: {traceback.format_exc()}"
            errors.append(msg)
            notifier.send_mail("二手车价格平台清洗异常", msg)

    if errors:
        return {"statusCode": 500, "body": "\n".join(errors)}
    return {"statusCode": 200, "body": f"完成: {', '.join(targets)}"}
