#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil

import pandas as pd

BASE = os.path.dirname(__file__)
PROCESSED_DIR = os.path.join(BASE, "..", "data", "processed")
DOCS = os.path.join(BASE, "..", "docs")

os.makedirs(DOCS, exist_ok=True)


def load_csv(name):
    path = os.path.join(PROCESSED_DIR, name)
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def tidy_overall(df, house_type):
    df = df.copy()
    df["house_type"] = house_type
    df["segment"] = "整体"
    df = df.rename(columns={"城市": "city"})
    return df[["month", "city", "house_type", "segment", "环比", "同比"]]


def tidy_area(df, house_type):
    df = df.copy()
    df["house_type"] = house_type
    df = df.rename(columns={"城市": "city"})
    # keep only area columns
    area_cols = [c for c in df.columns if "m2" in c and ("环比" in c or "同比" in c)]
    base_cols = ["month", "city", "house_type"]
    df = df[base_cols + area_cols]
    # reshape
    records = []
    for _, row in df.iterrows():
        for c in area_cols:
            val = row[c]
            if pd.isna(val) or val == "":
                continue
            # column format: 90m2及以下-环比
            if "-" in c:
                seg, metric = c.split("-", 1)
            else:
                seg, metric = "未知", c
            records.append({
                "month": row["month"],
                "city": row["city"],
                "house_type": house_type,
                "segment": seg,
                "metric": metric,
                "value": val,
            })
    return pd.DataFrame(records)


def build_chart_csv():
    new_overall = load_csv("new_home.csv")
    second_overall = load_csv("second_hand.csv")
    new_cat1 = load_csv("new_home_category_1.csv")
    new_cat2 = load_csv("new_home_category_2.csv")
    second_cat1 = load_csv("second_hand_category_1.csv")
    second_cat2 = load_csv("second_hand_category_2.csv")

    parts = []

    if new_overall is not None:
        df = tidy_overall(new_overall, "新房")
        df = df.melt(id_vars=["month", "city", "house_type", "segment"], value_vars=["环比", "同比"], var_name="metric", value_name="value")
        parts.append(df)
    if second_overall is not None:
        df = tidy_overall(second_overall, "二手房")
        df = df.melt(id_vars=["month", "city", "house_type", "segment"], value_vars=["环比", "同比"], var_name="metric", value_name="value")
        parts.append(df)

    if new_cat1 is not None:
        parts.append(tidy_area(new_cat1, "新房"))
    if new_cat2 is not None:
        parts.append(tidy_area(new_cat2, "新房"))
    if second_cat1 is not None:
        parts.append(tidy_area(second_cat1, "二手房"))
    if second_cat2 is not None:
        parts.append(tidy_area(second_cat2, "二手房"))

    if not parts:
        return

    chart = pd.concat(parts, ignore_index=True)
    chart = chart.drop_duplicates(subset=["month", "city", "house_type", "segment", "metric"], keep="last")
    chart.to_csv(os.path.join(DOCS, "chart.csv"), index=False)
    print("docs/chart.csv updated")

    # build relative price series from 环比 (base=100)
    price = chart[chart["metric"] == "环比"].copy()
    price["value"] = pd.to_numeric(price["value"], errors="coerce")
    price = price.dropna(subset=["value"])
    price = price.sort_values(["city", "house_type", "segment", "month"])
    out = []
    for (city, house, seg), grp in price.groupby(["city", "house_type", "segment"]):
        idx = 100.0
        for _, row in grp.iterrows():
            idx = idx * (row["value"] / 100.0)
            out.append({
                "month": row["month"],
                "city": city,
                "house_type": house,
                "segment": seg,
                "value": round(idx, 4),
            })
    pd.DataFrame(out).to_csv(os.path.join(DOCS, "price.csv"), index=False)
    print("docs/price.csv updated")


if __name__ == "__main__":
    build_chart_csv()
