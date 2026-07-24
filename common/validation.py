"""yck_car_basic_config 写入前数据验证。

拦截字段错位（车长跑进 emission、悬架跑进 abs、变速箱跑进 length 等）
和异常值，避免脏数据写入 IT 生产库。

用法（写库前）：
    from common.validation import validate_car_config
    clean_df, report = validate_car_config(df)
    report.log(logger)          # 打印/记录 ERROR、WARN 汇总
    # clean_df 已剔除 ERROR 行；WARN 行保留（仅留痕）
    write(clean_df)

规则依据：现有清洗代码的输出值域（autohome_match / vdatabase_sync，已与
原始 R 代码核对一致）+ 真实库数据分位数分布。分三级：
  - ERROR：铁定错位/非法 → 剔除该行
  - WARN ：可疑但可能合法（商用车/客车）→ 放行 + 留痕
  - 长度 ：超 varchar(N) → ERROR（MySQL 会截断）
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

# ── 字段最大长度（varchar(N)，超出即 ERROR）─────────────────────────────
MAX_LEN = {
    "mark": 4, "brand": 200, "series": 255, "is_selling": 10, "type_name": 255,
    "recommend_price": 20, "year": 4, "factory_name": 50, "produced_place": 30,
    "emission": 10, "level": 20, "engine": 50, "gear_box": 50,
    "length": 5, "width": 5, "height": 5, "body_structure": 30, "seat_number": 20,
    "wheelbase": 5, "weight": 5, "trunk_volume": 20, "intake": 30,
    "cylinder_arrangement": 4, "cylinders": 4, "admission_gear": 10,
    "max_ps": 5, "max_n-m": 5, "fuel": 10, "fuel_grade": 30, "fuel_supply_system": 20,
    "environmental_standards_org": 20, "drive_mode": 20,
    "front_suspension_type": 50, "rear_suspension_type": 50, "power_type": 30,
    "car_boty_type": 30, "front_brake_type": 30, "rear_brake_type": 30,
    "parking_brake_type": 30, "front_tire_specifications": 30, "rear_tire_specifications": 30,
    "variable_suspension": 30, "seat_material": 20, "low_beam_lamp": 30,
    "air_conditioning_control_mode": 30, "hl_configs": 200, "hl_configc": 200,
    "environmental_standards": 20,
}

# ── 配置标志位（清洗后仅 {Y, N, -}）─────────────────────────────────────
FLAG_COLS = [
    "master_air_bag", "sub_air_bag", "front_side_air_bag", "rear_side_air_bag",
    "front_head_air_bag", "rear_head_air_bag", "tire_pressure_monitoring",
    "ISOFIX_child_seat_interfaces", "car_internal_lock", "keyless_start_system",
    "abs", "brake_assist", "stability_control", "electric_roof", "panoramic_roof",
    "multifunction_steering_wheel", "cruise", "front_parking_radar", "rear_parking_radar",
    "reverse_video_image", "electric_seat_memory", "front_seat_heating", "rear_seat_heating",
    "front_seat_ventilation", "rear_seat_ventilation", "gps", "daytime_running_light",
    "automatic_headlights", "front_fog_lamp", "front_electric_window", "rear_electric_window",
    "mirror_electric_adjustment", "mirror_heating",
]
_FLAG_OK = {"Y", "N", "-", ""}

# 错位污染特征词（出现在标志位/相邻字段=错位）
_SUSPENSION_WORDS = re.compile(r"多连杆|承载式|双横臂|麦弗逊|扭力梁|悬架|独立悬")
_BRAKE_WORDS = re.compile(r"通风盘式|盘式|鼓式")
_DRIVE_WORDS = re.compile(r"前置前驱|前置后驱|前置四驱|后置|中置|四驱|两驱|适时四驱|全时四驱")
_CONFIG_WORDS = re.compile(r"标配|选配|驻车")
# 配置标志位/进气等错位进 engine 的特征（engine 正常含「马力/电动/kW」）
_ENGINE_DIRTY = re.compile(r"标配|选配|^[YN]$|^\d+$|直喷|多点电喷|混合喷射")

# level（车型等级）合法枚举：中文等级词 + 无值表示
_LEVEL_OK = {
    "紧凑型SUV", "中型SUV", "紧凑型车", "中大型车", "中型车", "中大型SUV", "大型SUV",
    "大型车", "微型车", "小型车", "MPV", "跑车", "小型SUV", "微面", "皮卡", "微卡",
    "轻客", "客车", "紧凑型MPV", "卡车", "中大型MPV", "货车", "重卡",
    "无", "-", "",  # 无值表示
}

# ── 数值字段三层范围：(正常min, 正常max, 商用车max)。超商用车max或非数字=ERROR ──
#   在 [正常max, 商用车max] 之间 = WARN（商用车/客车），(0,正常min) 也 WARN
NUM_RANGE = {
    #             正常min 正常max 商用车max
    "length":     (2000,  5800,   13000),
    "width":      (1400,  2100,   3000),
    "height":     (1200,  2200,   4000),
    "wheelbase":  (1800,  3800,   9000),
    "seat_number": (2,     9,      60),
    "cylinders":  (2,     8,      16),
    "max_ps":     (20,    600,    2000),
}


@dataclass
class Report:
    total: int = 0
    kept: int = 0
    dropped: int = 0
    errors: list = field(default_factory=list)   # (row_idx, autohome_id, 字段, 原因)
    warns: list = field(default_factory=list)

    def log(self, logger) -> None:
        logger.info(f"  数据验证: 共 {self.total} 行，通过 {self.kept}，剔除(ERROR) {self.dropped}")
        if self.errors:
            logger.warning(f"  ✗ ERROR {len(self.errors)} 处（对应行已剔除）:")
            for _, aid, col, why in self.errors[:50]:
                logger.warning(f"      autohome_id={aid} [{col}] {why}")
            if len(self.errors) > 50:
                logger.warning(f"      ... 另有 {len(self.errors) - 50} 处")
        if self.warns:
            logger.info(f"  ⚠ WARN {len(self.warns)} 处（已放行，供核查）:")
            for _, aid, col, why in self.warns[:30]:
                logger.info(f"      autohome_id={aid} [{col}] {why}")
            if len(self.warns) > 30:
                logger.info(f"      ... 另有 {len(self.warns) - 30} 处")

    def error_ids(self) -> list:
        return sorted({aid for _, aid, _, _ in self.errors})


def _s(v) -> str:
    """转字符串并去首尾空格。库里的尾随空格（如 'Y '）是格式瑕疵，
    校验时按 trim 后判定，避免误报为脏数据。"""
    return "" if pd.isna(v) else str(v).strip()


def _num(v):
    m = re.match(r"^\s*(\d+(?:\.\d+)?)", _s(v))
    return float(m.group(1)) if m else None


def validate_car_config(df: pd.DataFrame) -> tuple[pd.DataFrame, Report]:
    """校验宽表。返回 (剔除 ERROR 行后的 df, 报告)。"""
    rep = Report(total=len(df))
    if df.empty:
        rep.kept = 0
        return df, rep

    bad_rows: set = set()

    def err(idx, aid, col, why):
        rep.errors.append((idx, aid, col, why))
        bad_rows.add(idx)

    def warn(idx, aid, col, why):
        rep.warns.append((idx, aid, col, why))

    for idx, row in df.iterrows():
        aid = _s(row.get("autohome_id"))

        # 1) 标识/关联字段必填且为数字
        for col in ("autohome_id", "brandid", "series_id"):
            if col in df.columns and not re.fullmatch(r"\d+", _s(row.get(col))):
                err(idx, aid, col, f"应为数字，实际 {row.get(col)!r}")
        for col in ("brand", "series", "type_name"):
            if col in df.columns and _s(row.get(col)).strip() in ("", "-"):
                err(idx, aid, col, "关键字段为空")

        # 2) 长度约束
        for col, mx in MAX_LEN.items():
            if col in df.columns and len(_s(row.get(col))) > mx:
                err(idx, aid, col, f"超长 {len(_s(row.get(col)))}>{mx}: {_s(row.get(col))[:30]!r}")

        # 3) 枚举字段
        if "is_selling" in df.columns and _s(row.get("is_selling")) not in ("在售", "停售", "即将销售", ""):
            err(idx, aid, "is_selling", f"非法状态: {row.get('is_selling')!r}")
        if "is_green" in df.columns and _s(row.get("is_green")) not in ("0", "1", "2", "3", "4", "5", "6", ""):
            err(idx, aid, "is_green", f"非法: {row.get('is_green')!r}")

        # 4) 配置标志位：仅 {Y,N,-}，出现悬架/制动词=错位
        for col in FLAG_COLS:
            if col not in df.columns:
                continue
            val = _s(row.get(col))
            if val in _FLAG_OK:
                continue
            if _SUSPENSION_WORDS.search(val) or _BRAKE_WORDS.search(val):
                err(idx, aid, col, f"标志位混入悬架/制动值（错位）: {val!r}")
            elif re.search(r"[YN]/[YN]", val):
                warn(idx, aid, col, f"未拆分值: {val!r}")
            else:
                err(idx, aid, col, f"非 Y/N/- 值: {val!r}")

        # 5) 错位特征字段
        if "length" in df.columns and re.search(r"[^\d.]", _s(row.get("length"))) and _s(row.get("length")) not in ("", "-"):
            err(idx, aid, "length", f"含非数字（变速箱/发动机错位）: {_s(row.get('length'))!r}")
        if "emission" in df.columns and re.fullmatch(r"\d{4}", _s(row.get("emission"))):
            err(idx, aid, "emission", f"纯4位数（车长错位）: {row.get('emission')!r}")
        if "gear_box" in df.columns and re.fullmatch(r"\d{3,4}", _s(row.get("gear_box"))):
            err(idx, aid, "gear_box", f"纯数字（车宽错位）: {row.get('gear_box')!r}")
        if "fuel" in df.columns and _BRAKE_WORDS.search(_s(row.get("fuel"))):
            err(idx, aid, "fuel", f"混入制动词（错位）: {row.get('fuel')!r}")
        if "environmental_standards" in df.columns and _DRIVE_WORDS.search(_s(row.get("environmental_standards"))):
            err(idx, aid, "environmental_standards", f"混入驱动词（错位）: {row.get('environmental_standards')!r}")
        if "front_tire_specifications" in df.columns and _CONFIG_WORDS.search(_s(row.get("front_tire_specifications"))):
            warn(idx, aid, "front_tire_specifications", f"含配置词（疑错位）: {row.get('front_tire_specifications')!r}")

        # level：车型等级，只能是中文等级枚举或无值；数字/其它=错位
        if "level" in df.columns:
            lv = _s(row.get("level"))
            if lv not in _LEVEL_OK:
                err(idx, aid, "level", f"非法车型等级（应为等级词或无）: {lv!r}")

        # engine：正常含「马力/电动/kW」；出现 标配/Y/N/纯数字/进气词 = 错位
        if "engine" in df.columns:
            eng = _s(row.get("engine"))
            if eng not in ("", "-", "未知") and _ENGINE_DIRTY.search(eng):
                err(idx, aid, "engine", f"疑似错位（含配置/纯数字/进气值）: {eng!r}")

        # 6) 数值三层范围
        for col, (nmin, nmax, cmax) in NUM_RANGE.items():
            if col not in df.columns:
                continue
            raw = _s(row.get(col))
            if raw in ("", "-"):
                continue
            n = _num(raw)
            if n is None:  # 数值字段却非数字 = 错位
                err(idx, aid, col, f"应为数值实际 {raw!r}（错位）")
            elif n > cmax or n <= 0:
                err(idx, aid, col, f"超商用车合理范围: {n}（正常{nmin}~{nmax}）")
            elif n < nmin or n > nmax:
                warn(idx, aid, col, f"超乘用车范围（商用车？）: {n}")

        # 7) 价格/年款
        if "recommend_price" in df.columns:
            p = _num(row.get("recommend_price"))
            if p is not None and (p <= 0 or p > 2000):
                warn(idx, aid, "recommend_price", f"指导价异常: {p} 万")
        if "year" in df.columns:
            y = _num(row.get("year"))
            if y is not None and (y < 1990 or y > 2030):
                err(idx, aid, "year", f"年款异常: {row.get('year')!r}")

    rep.dropped = len(bad_rows)
    rep.kept = rep.total - rep.dropped
    clean = df.drop(index=list(bad_rows)) if bad_rows else df
    return clean, rep
