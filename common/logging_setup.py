"""统一日志配置。

所有模块用 `get_logger(__name__)` 取 logger，输出到 stdout，
便于 Airflow / 容器自动收集 task 日志。

格式：2026-06-17 11:22:33 [INFO] autohome_match: 消息内容
"""
import logging
import os
import sys

_CONFIGURED = False


def _configure_root() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root = logging.getLogger()
    # 避免重复 handler（Airflow / 多次导入场景）
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """取一个已配置的 logger。name 通常传 __name__。"""
    _configure_root()
    # 用模块短名（去掉包前缀）让日志更易读：autohome_match.handler → autohome_match
    short = name.split(".")[0] if "." in name else name
    return logging.getLogger(short)
