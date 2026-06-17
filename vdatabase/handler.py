from __future__ import annotations

import pandas as pd

from common import config, db, notifier


def run() -> None:
    _sync_brand()
    _sync_series()
    _sync_model()
    _update_major_info()
    notifier.send_mail("main_che300 执行完成", "车型库ID自增处理完成")
    print("[vdatabase] 完成。")


# ────────────────────────────────────────────────
# 品牌 ID 自增
# ────────────────────────────────────────────────

def _sync_brand() -> None:
    brand_add = db.query(
        config.DB_YUN,
        """SELECT a.Initial, a.brandid, a.brand_name FROM
           (SELECT DISTINCT Initial, brandid, brand_name FROM config_che300_major_info) a
           LEFT JOIN config_vdatabase_yck_brand b ON a.brandid = b.brandid
           WHERE yck_brandid IS NULL""",
    )
    if brand_add.empty:
        return

    brand_all = db.query(
        config.DB_YUN,
        """SELECT a.Initial, a.brandid, a.brand_name, yck_brandid FROM
           (SELECT DISTINCT Initial, brandid, brand_name FROM config_che300_major_info) a
           INNER JOIN config_vdatabase_yck_brand b ON a.brandid = b.brandid""",
    )
    brand_all["yck_num"] = brand_all["yck_brandid"].str.replace(r"[a-zA-Z]", "", regex=True).astype(int)
    max_per_initial = brand_all.groupby("Initial")["yck_num"].max().reset_index(name="maxb")

    brand_add = brand_add.merge(max_per_initial, on="Initial", how="left")
    brand_add["maxb"] = brand_add["maxb"].fillna(0).astype(int)
    brand_add["rank"] = brand_add.groupby("Initial")["brand_name"].transform(
        lambda s: pd.factorize(s)[0] + 1
    )
    brand_add["yck_brandid"] = brand_add["Initial"] + (brand_add["maxb"] + brand_add["rank"]).astype(str)
    brand_add = brand_add[["brandid", "Initial", "brand_name", "yck_brandid"]].copy()
    brand_add["car_country"] = "无"

    db.bulk_upsert(config.DB_YUN, "config_vdatabase_yck_brand", brand_add)


# ────────────────────────────────────────────────
# 车系 ID 自增
# ────────────────────────────────────────────────

def _sync_series() -> None:
    series_add = db.query(
        config.DB_YUN,
        """SELECT a.series_id, a.series_group_name, a.series_name, yck_brandid FROM
           (SELECT DISTINCT brandid, series_id, series_group_name, series_name FROM config_che300_major_info) a
           LEFT JOIN config_vdatabase_yck_series b ON a.series_id = b.series_id
           INNER JOIN config_vdatabase_yck_brand c ON a.brandid = c.brandid
           WHERE yck_seriesid IS NULL""",
    )
    if series_add.empty:
        return

    series_all = db.query(
        config.DB_YUN,
        """SELECT yck_brandid, yck_seriesid FROM
           (SELECT DISTINCT brandid, series_id, series_name FROM config_che300_major_info) a
           INNER JOIN config_vdatabase_yck_series b ON a.series_id = b.series_id
           INNER JOIN config_vdatabase_yck_brand c ON a.brandid = c.brandid""",
    )

    # 进口车系名前缀处理
    is_import_mask = series_add["series_name"].str.contains("进口", na=False)
    series_add.loc[is_import_mask, "series_group_name"] = (
        "进口" + series_add.loc[is_import_mask, "series_group_name"]
    )
    series_add["series_group_name"] = (
        series_add["series_group_name"]
        .str.replace("进口进口进口|进口进口", "进口", regex=True)
    )
    series_add["series_name"] = series_add["series_name"].str.replace(r"\(进口\)", "", regex=True)
    series_add["series_name"] = series_add["series_name"].str.replace("欧尚COSMOS(科尚)", "欧尚COSMOS-科尚", regex=False)
    series_add["series_name"] = series_add["series_name"].str.upper()

    is_import = series_add["series_group_name"].str.extract(r"(进口)")[0].fillna("0")
    is_import = is_import.replace("进口", "1")

    series_all["yck_num"] = series_all["yck_seriesid"].str.extract(r"(\d+)$")[0].astype(int)
    max_per_brand = series_all.groupby("yck_brandid")["yck_num"].max().reset_index(name="maxs")

    series_add = series_add.merge(max_per_brand, on="yck_brandid", how="left")
    series_add["maxs"] = series_add["maxs"].fillna(0).astype(int)
    series_add["rank"] = series_add.groupby("yck_brandid")["series_name"].transform(
        lambda s: pd.factorize(s)[0] + 1
    )
    series_add["yck_seriesid"] = series_add["yck_brandid"] + "S" + (series_add["maxs"] + series_add["rank"]).astype(str)
    series_add["is_import"] = is_import.values
    series_add["car_level"] = "无"
    series_add["is_green"] = "无"
    series_add = series_add[["series_id", "series_group_name", "series_name", "yck_seriesid", "is_import", "car_level", "is_green"]]

    db.bulk_upsert(config.DB_YUN, "config_vdatabase_yck_series", series_add)

    # 更新 car_level / is_green（逐车系从最新年款众数填充）
    series_deal = db.query(
        config.DB_YUN,
        "SELECT series_id FROM yckdc_da2_ucar.config_vdatabase_yck_series WHERE car_level='无' OR is_green='无'",
    )
    for sid in series_deal["series_id"].tolist():
        tmp = db.query(
            config.DB_YUN,
            f"""SELECT car_level, is_green, COUNT(*) cnt
                FROM yckdc_da2_ucar.config_che300_major_info
                WHERE series_id={sid}
                  AND model_year = (SELECT MAX(model_year) FROM yckdc_da2_ucar.config_che300_major_info WHERE series_id={sid})
                GROUP BY car_level, is_green ORDER BY cnt LIMIT 1""",
        )
        if not tmp.empty:
            cl, ig = tmp["car_level"].iloc[0], tmp["is_green"].iloc[0]
            db.execute(
                config.DB_YUN,
                f"UPDATE yckdc_da2_ucar.config_vdatabase_yck_series SET car_level='{cl}', is_green='{ig}' WHERE series_id={sid}",
            )


# ────────────────────────────────────────────────
# 车型 ID 自增
# ────────────────────────────────────────────────

def _sync_model() -> None:
    model_add = db.query(
        config.DB_YUN,
        """SELECT a.model_id, a.Initial, a.brandid, d.yck_brandid, a.series_id, c.yck_seriesid,
                  a.auto, a.liter, a.is_green, 0 is_check
           FROM config_che300_major_info a
           LEFT JOIN config_vdatabase_yck_model b ON a.model_id = b.model_id
           LEFT JOIN config_vdatabase_yck_series c ON a.series_id = c.series_id
           LEFT JOIN config_vdatabase_yck_brand d ON a.brandid = d.brandid
           WHERE b.model_id IS NULL""",
    )
    if model_add.empty:
        return

    model_all = db.query(
        config.DB_YUN,
        """SELECT c.yck_seriesid, a.yck_modelid
           FROM config_vdatabase_yck_model a
           INNER JOIN config_che300_major_info b ON a.model_id = b.model_id
           INNER JOIN config_vdatabase_yck_series c ON b.series_id = c.series_id""",
    )
    model_all["yck_num"] = model_all["yck_modelid"].str.extract(r"(\d+)$")[0].astype(int)
    max_per_series = model_all.groupby("yck_seriesid")["yck_num"].max().reset_index(name="maxs")

    model_add = model_add.merge(max_per_series, on="yck_seriesid", how="left")
    model_add["maxs"] = model_add["maxs"].fillna(0).astype(int)
    model_add["rank"] = model_add.groupby("yck_seriesid")["model_id"].transform(
        lambda s: pd.factorize(s)[0] + 1
    )
    model_add["yck_modelid"] = model_add["yck_seriesid"] + "M" + (model_add["maxs"] + model_add["rank"]).astype(str)
    model_add = model_add[["yck_modelid", "model_id", "auto", "liter", "is_green", "is_check"]]
    db.bulk_upsert(config.DB_YUN, "config_vdatabase_yck_model", model_add)


# ────────────────────────────────────────────────
# 车型库宽表更新
# ────────────────────────────────────────────────

def _update_major_info() -> None:
    model_config = db.query(
        config.DB_YUN,
        """SELECT a.yck_modelid, a.model_id, b.Initial, b.brandid, c.yck_brandid, c.brand_name,
                  d.series_group_name, b.series_id, d.yck_seriesid, d.series_name,
                  b.model_name, b.short_name, b.model_price, b.model_year, a.auto, a.liter,
                  b.discharge_standard, b.max_reg_year, b.min_reg_year, d.car_level,
                  b.seat_number, b.hl_configs, b.hl_configc, a.is_green, d.is_import
           FROM config_vdatabase_yck_model a
           INNER JOIN config_che300_major_info b ON a.model_id = b.model_id
           INNER JOIN config_vdatabase_yck_brand c ON b.brandid = c.brandid
           INNER JOIN config_vdatabase_yck_series d ON b.series_id = d.series_id
           WHERE d.car_level != '无'""",
    )
    if not model_config.empty:
        db.bulk_upsert(config.DB_YUN, "config_vdatabase_yck_major_info", model_config)

    # 更新近年最大注册年份
    db.execute(
        config.DB_YUN,
        """UPDATE config_vdatabase_yck_major_info a,
           (SELECT model_id, max_reg_year FROM config_che300_major_info WHERE max_reg_year >= 2019) b
           SET a.max_reg_year = b.max_reg_year
           WHERE a.model_id = b.model_id AND a.max_reg_year >= 2019""",
    )


if __name__ == "__main__":
    run()
