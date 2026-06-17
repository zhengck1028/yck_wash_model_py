import smtplib
import traceback
from email.mime.text import MIMEText

from common.config import SMTP


def send_mail(subject: str, body: str = "") -> None:
    msg = MIMEText(body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP["from"]
    msg["To"] = ", ".join(SMTP["to"])
    try:
        with smtplib.SMTP_SSL(SMTP["host"], SMTP["port"]) as server:
            server.login(SMTP["user"], SMTP["password"])
            server.sendmail(SMTP["from"], SMTP["to"], msg.as_string())
    except Exception:
        print(f"[notifier] 邮件发送失败:\n{traceback.format_exc()}")


def notify_on_error(module_name: str):
    """装饰器：捕获异常并发送邮件告警"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                send_mail(
                    subject=f"{module_name} 执行异常",
                    body=f"{module_name} 失败，请及时处理：<br>{traceback.format_exc()}",
                )
                raise
        return wrapper
    return decorator
