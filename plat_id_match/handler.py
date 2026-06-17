"""
平台 ID 匹配模块

对应 R 的 main_plat_id.R + matchFun_vdatabase.R。

算法：多轮 pandas merge 精确匹配，逐步减少字段数量，收敛到唯一匹配。
匹配方向：平台车型（待匹配）→ 车300标准车型库（che300）
"""
from __future__ import annotations

import re
from functools import reduce

import pandas as pd

from common import config, db, notifier

# 需要匹配的平台配置：(数据库源表名, 平台短名)
PLATFORMS = [
    ("config_autohome_major_info_tmp", "autohome"),
    ("config_chezhibao_major_info",    "czb"),
    ("config_souhu_major_info",        "souhu"),
    ("config_yiche_major_info",        "yiche"),
    ("config_che58_major_info",        "che58"),
    ("config_youxin_major_info_tmp",   "youxin"),
    ("config_autoowner_major_info_tmp","autoowner"),
]

# 停用词/归一化规则（简化版，关键规则内联，完整规则可从 CSV 扩展）
_AUTO_MAP = {
    r"CVT无级变速|无级变速|CVT": "自动",
    r"双离合": "自动双离合",
    r"手自一体": "自动",
    r"^[Aa]uto.*": "自动",
    r"^[Mm]anual.*": "手动",
}
_DRIVE_MAP = {r"四驱|AWD|4WD|4X4|4x4": "四驱", r"两驱|FWD|RWD|2WD": "两驱"}

# 用于精确匹配的关键字段顺序（重要性递减）
_MATCH_FIELDS = [
    "car_year", "car_name", "car_series1",
    "car_liter_wu", "car_auto", "car_price",
    "car_drive", "car_discharge",
]


def run() -> None:
    che300 = db.query(config.DB_YUN, "SELECT * FROM analysis_che300_cofig_info")
    rm_series_rule = db.query(config.DB_YUN, "SELECT * FROM config_reg_series_rule")
    id_z = che300[["car_id"]].rename(columns={"car_id": "id_che300"})

    results: dict[str, pd.DataFrame] = {}
    for tbl, name in PLATFORMS:
        try:
            results[name] = _match_platform(tbl, name, che300, rm_series_rule)
            print(f"[plat_id_match] {name} 匹配完成，{len(results[name])} 行")
        except Exception as e:
            notifier.send_mail(f"车型库匹配异常 {name}", str(e))
            results[name] = pd.DataFrame(columns=["id_che300", f"id_{name}", f"match_type_{name}", f"is_only_{name}"])

    # 合并所有平台结果
    id_result = id_z.copy()
    for name, res in results.items():
        res["id_che300"] = res["id_che300"].astype(str)
        id_result["id_che300"] = id_result["id_che300"].astype(str)
        id_result = id_result.merge(res, on="id_che300", how="left")

    id_result = id_result.fillna(0).drop_duplicates()
    db.bulk_upsert(config.DB_YUN, "config_plat_id_match", id_result)
    notifier.send_mail("平台ID匹配完成", f"共匹配 {len(id_result)} 条")
    print(f"[plat_id_match] 完成，写入 {len(id_result)} 行。")


def _match_platform(
    table_name: str,
    platform_name: str,
    che300: pd.DataFrame,
    rm_series_rule: pd.DataFrame,
) -> pd.DataFrame:
    id_col = f"id_{platform_name}"
    match_col = f"match_type_{platform_name}"
    only_col = f"is_only_{platform_name}"

    # 已匹配记录（跳过）
    id_benchmark = db.query(
        config.DB_YUN,
        f"SELECT id_che300, {id_col}, {match_col}, {only_col} FROM config_plat_id_match WHERE {match_col} NOT IN (0, 4)",
    )

    # 拉取平台待匹配数据
    plat_raw = db.query(
        config.DB_YUN,
        f"""SELECT model_id car_id, brand_name, series_name, model_name model_name_t,
                   model_price, model_year, liter, auto car_auto, discharge_standard, series_id
            FROM {table_name}""",
    )

    # 车系名品牌修正（借助已匹配车系的 YCK 标准名）
    match_series = db.query(
        config.DB_YUN,
        f"""SELECT DISTINCT a.series_id, c.brand_name temp_brand, c.series_name temp_series
            FROM {table_name} a
            INNER JOIN config_plat_id_match b ON a.model_id = b.{id_col}
            INNER JOIN config_vdatabase_yck_major_info c ON b.id_che300 = c.model_id
            GROUP BY series_id""",
    )
    plat_raw = plat_raw.merge(match_series, on="series_id", how="left")
    mask = plat_raw["temp_brand"].notna()
    plat_raw.loc[mask, "brand_name"] = plat_raw.loc[mask, "temp_brand"]
    plat_raw.loc[mask, "series_name"] = plat_raw.loc[mask, "temp_series"]
    plat_raw = plat_raw.drop(columns=["series_id", "temp_brand", "temp_series"])

    # 去除已匹配车型
    if not id_benchmark.empty:
        plat_raw = plat_raw[~plat_raw["car_id"].astype(str).isin(id_benchmark[id_col].astype(str))]
        che300_filt = che300[~che300["car_id"].astype(str).isin(id_benchmark["id_che300"].astype(str))]
    else:
        che300_filt = che300.copy()

    if plat_raw.empty:
        return _empty_result(id_benchmark, id_col, match_col, only_col)

    # 特征提取（停用词清洗）
    plat_feat = _extract_features(plat_raw, id_col="car_id")
    che300_feat = _extract_features(che300_filt, id_col="car_id")

    # 多轮精确匹配
    matched = _multi_round_match(plat_feat, che300_feat)

    # 重复匹配二次筛选
    right = matched[matched["match_des"] == "right"].copy()
    right = _post_filter_right(right, plat_feat, che300_feat)

    repeat_ = matched[matched["match_des"] == "repeat"].copy()
    repeat_resolved = _resolve_repeat(repeat_, plat_feat, che300_feat)

    final_matched = pd.concat([right, repeat_resolved], ignore_index=True)
    final_matched = final_matched[["id_che300", "platform_id", "match_type", "is_only"]]
    final_matched.columns = ["id_che300", id_col, match_col, only_col]

    return _merge_with_benchmark(final_matched, id_benchmark, id_col, match_col, only_col, id_z_base=che300[["car_id"]].rename(columns={"car_id": "id_che300"}))


# ────────────────────────────────────────────────
# 特征提取（等价 fun_stopWords 简化版）
# ────────────────────────────────────────────────

def _extract_features(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    feat = pd.DataFrame()
    feat["car_id"] = df[id_col].astype(str)

    raw_name = (df.get("model_name_t") if "model_name_t" in df.columns else df.get("model_name", "")).fillna("")
    brand = df.get("brand_name", pd.Series("", index=df.index)).fillna("").str.upper()
    series = df.get("series_name", pd.Series("", index=df.index)).fillna("").str.upper()

    feat["car_name"] = brand
    feat["car_series1"] = series
    feat["car_year"] = pd.to_numeric(df.get("model_year", 0), errors="coerce").fillna(0).astype(int)
    feat["car_price"] = pd.to_numeric(df.get("model_price", 0), errors="coerce").fillna(0).round(1)
    feat["car_liter_wu"] = (
        df.get("liter", pd.Series("", index=df.index)).astype(str)
        .str.extract(r"(\d+\.?\d*)")[0].fillna("")
    )
    feat["car_auto"] = _normalize_series(df.get("car_auto", pd.Series("", index=df.index)).fillna("").astype(str), _AUTO_MAP)
    feat["car_drive"] = ""
    feat["car_discharge"] = (
        df.get("discharge_standard", pd.Series("", index=df.index)).fillna("").astype(str)
        .str.replace(r"\+OBD|OBD", "", regex=True)
    )
    return feat.reset_index(drop=True)


def _normalize_series(s: pd.Series, mapping: dict) -> pd.Series:
    for pattern, repl in mapping.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s


# ────────────────────────────────────────────────
# 多轮精确匹配
# ────────────────────────────────────────────────

def _multi_round_match(plat: pd.DataFrame, che300: pd.DataFrame) -> pd.DataFrame:
    """逐步减少匹配字段，收敛到唯一匹配"""
    remaining_plat = plat.copy()
    results = []

    field_groups = [
        _MATCH_FIELDS,
        _MATCH_FIELDS[:6],
        _MATCH_FIELDS[:5],
        _MATCH_FIELDS[:4],
        _MATCH_FIELDS[:3] + [_MATCH_FIELDS[5]],  # 年款+品牌+车系+价格
        _MATCH_FIELDS[:3],
    ]

    for fields in field_groups:
        if remaining_plat.empty:
            break
        merged = remaining_plat.merge(
            che300.rename(columns={f: f + "_c300" for f in fields + ["car_id"]}),
            left_on=fields,
            right_on=[f + "_c300" for f in fields],
            how="inner",
        )
        if merged.empty:
            continue
        cnt = merged.groupby("car_id")["car_id_c300"].transform("count")
        right = merged[cnt == 1].copy()
        right["match_des"] = "right"
        repeat = merged[cnt > 1].copy()
        repeat["match_des"] = "repeat"
        found = pd.concat([right, repeat])
        found = found[["car_id", "car_id_c300", "match_des"]].rename(
            columns={"car_id": "platform_id", "car_id_c300": "id_che300"}
        )
        results.append(found)
        matched_ids = found["platform_id"].unique()
        remaining_plat = remaining_plat[~remaining_plat["car_id"].isin(matched_ids)]

    if not results:
        return pd.DataFrame(columns=["platform_id", "id_che300", "match_des"])
    return pd.concat(results, ignore_index=True).drop_duplicates()


# ────────────────────────────────────────────────
# 匹配后处理
# ────────────────────────────────────────────────

def _post_filter_right(
    right: pd.DataFrame, plat_feat: pd.DataFrame, che300_feat: pd.DataFrame
) -> pd.DataFrame:
    if right.empty:
        return pd.DataFrame(columns=["id_che300", "platform_id", "match_type", "is_only"])
    right = right.merge(
        plat_feat[["car_id", "car_year", "car_price"]].rename(columns={"car_id": "platform_id", "car_year": "p_year", "car_price": "p_price"}),
        on="platform_id",
    ).merge(
        che300_feat[["car_id", "car_year", "car_price"]].rename(columns={"car_id": "id_che300", "car_year": "c_year", "car_price": "c_price"}),
        on="id_che300",
    )
    right = right[((right["p_year"] - right["c_year"]).abs() + (right["p_price"] - right["c_price"]).abs()).round(1) == 0]
    right = right.groupby("platform_id").filter(lambda g: len(g) == 1)
    right["match_type"] = right.groupby("platform_id")["id_che300"].transform("count").apply(lambda n: 1 if n == 1 else 2)
    right["is_only"] = right.groupby("platform_id")["id_che300"].transform("min") == right["id_che300"]
    right["is_only"] = right["is_only"].astype(int)
    return right[["id_che300", "platform_id", "match_type", "is_only"]]


def _resolve_repeat(
    repeat: pd.DataFrame, plat_feat: pd.DataFrame, che300_feat: pd.DataFrame
) -> pd.DataFrame:
    if repeat.empty:
        return pd.DataFrame(columns=["id_che300", "platform_id", "match_type", "is_only"])

    extra_plat = plat_feat[["car_id", "car_year", "car_price", "car_auto", "car_drive"]].rename(
        columns={"car_id": "platform_id", "car_year": "p_year", "car_price": "p_price",
                 "car_auto": "p_auto", "car_drive": "p_drive"}
    )
    extra_c300 = che300_feat[["car_id", "car_year", "car_price", "car_auto", "car_drive"]].rename(
        columns={"car_id": "id_che300", "car_year": "c_year", "car_price": "c_price",
                 "car_auto": "c_auto", "car_drive": "c_drive"}
    )
    df = repeat.merge(extra_plat, on="platform_id").merge(extra_c300, on="id_che300")
    df = df[((df["p_year"] - df["c_year"]).abs() + (df["p_price"] - df["c_price"]).abs()).round(1) == 0]

    resolved = []
    # 轮 1：年款+价格+变速+配置名+座位一致
    r1 = df[df["p_auto"] == df["c_auto"]]
    r1 = r1.groupby("platform_id").filter(lambda g: len(g) == 1)
    resolved.append(r1)
    remaining = df[~df["platform_id"].isin(r1["platform_id"])]

    # 轮 2：年款+价格+驱动一致
    r2 = remaining[remaining["p_drive"] == remaining["c_drive"]]
    r2 = r2.groupby("platform_id").filter(lambda g: len(g) == 1)
    resolved.append(r2)
    remaining = remaining[~remaining["platform_id"].isin(r2["platform_id"])]

    # 轮 3：仅年款+价格
    r3 = remaining.groupby("platform_id").filter(lambda g: len(g) == 1)
    resolved.append(r3)

    if not resolved:
        return pd.DataFrame(columns=["id_che300", "platform_id", "match_type", "is_only"])

    out = pd.concat(resolved, ignore_index=True)[["id_che300", "platform_id"]].drop_duplicates()
    out["match_type"] = out.groupby("platform_id")["id_che300"].transform("count").apply(lambda n: 1 if n == 1 else 2)
    out["is_only"] = (out.groupby("platform_id")["id_che300"].transform("min") == out["id_che300"]).astype(int)
    return out[["id_che300", "platform_id", "match_type", "is_only"]]


def _merge_with_benchmark(
    new_match: pd.DataFrame,
    id_benchmark: pd.DataFrame,
    id_col: str,
    match_col: str,
    only_col: str,
    id_z_base: pd.DataFrame,
) -> pd.DataFrame:
    """将新匹配结果与历史已匹配结果合并"""
    result = id_z_base.copy()
    result["id_che300"] = result["id_che300"].astype(str)
    if not id_benchmark.empty:
        id_benchmark["id_che300"] = id_benchmark["id_che300"].astype(str)
        result = result.merge(id_benchmark[["id_che300", id_col, match_col, only_col]], on="id_che300", how="left")
    else:
        result[id_col] = None
        result[match_col] = None
        result[only_col] = None

    if not new_match.empty:
        new_match["id_che300"] = new_match["id_che300"].astype(str)
        mask_null = result[id_col].isna()
        result.loc[mask_null] = result.loc[mask_null].drop(columns=[id_col, match_col, only_col]).merge(
            new_match, on="id_che300", how="left"
        )
    return result


def _empty_result(
    id_benchmark: pd.DataFrame, id_col: str, match_col: str, only_col: str
) -> pd.DataFrame:
    if id_benchmark.empty:
        return pd.DataFrame(columns=["id_che300", id_col, match_col, only_col])
    return id_benchmark


if __name__ == "__main__":
    run()
