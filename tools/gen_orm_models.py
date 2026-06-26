"""逆向工程 ORM 模型：从真实库反射表结构，生成声明式 SQLAlchemy 模型。

按业务域分组，每组生成一个模型文件到 common/models/。表结构变更后
重新运行本脚本即可刷新模型（生成的文件不要手改，改这里的表清单）。

用法（项目根目录执行）：
  python tools/gen_orm_models.py

依赖：sqlacodegen（pip install sqlacodegen）
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import MetaData
from sqlacodegen.generators import DeclarativeGenerator

from common import config, db

OUT_DIR = Path(__file__).resolve().parents[1] / "common" / "models"

# 业务域 → (数据库配置, 文件名, 表清单)
GROUPS = [
    (
        config.DB_IT,
        "it_car.py",
        [
            "yck_car_basic_brand",
            "yck_car_basic_series",
            "yck_car_basic_config",
            "yck_car_basic_config_ev",
        ],
    ),
    (
        config.DB_ODS,
        "ods_autohome.py",
        [
            "config_autohome_major_info_tmp",
            "config_autohome_detail_info",
            "config_autohome_ev_info",
            "config_autohome_yck_brand",
            "config_autohome_yck_series",
        ],
    ),
]

_HEADER = '"""自动生成的 ORM 模型——请勿手改，改 tools/gen_orm_models.py 后重新生成。"""\n'


def _generate(cfg: dict, filename: str, tables: list[str]) -> None:
    eng = db.get_engine(cfg)
    md = MetaData()
    md.reflect(bind=eng, only=tables)
    gen = DeclarativeGenerator(md, eng, options=set())
    code = gen.generate()
    out = OUT_DIR / filename
    out.write_text(_HEADER + code, encoding="utf-8")
    print(f"  {filename}: {len(md.tables)} 表, {len(code.splitlines())} 行")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    init = OUT_DIR / "__init__.py"
    if not init.exists():
        init.write_text('"""逆向工程的 SQLAlchemy ORM 模型。"""\n', encoding="utf-8")
    print("生成 ORM 模型...")
    for cfg, filename, tables in GROUPS:
        _generate(cfg, filename, tables)
    print("完成。")


if __name__ == "__main__":
    main()
