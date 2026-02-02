#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import re
import sys
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

API_URL = "https://api.so-gov.cn/query/s"
SITE_CODE = "bm36000002"
QUERY = "70个大中城市商品住宅销售价格变动"

TABLE_NAMES = [
    "new_home",
    "second_hand",
    "new_home_category_1",
    "new_home_category_2",
    "second_hand_category_1",
    "second_hand_category_2",
]

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
INDEX_FILE = os.path.join(PROCESSED_DIR, "index.json")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


def post_json(url, data, retries=3):
    encoded = urlencode(data).encode("utf-8")
    req = Request(url, data=encoded, headers={"User-Agent": "Mozilla/5.0"})
    last_err = None
    for _ in range(retries):
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8", errors="ignore"))
        except Exception as e:
            last_err = e
    raise last_err


def clean_html(text):
    if text is None:
        return ""
    text = re.sub(r"<[^>]+>", "", str(text))
    return text.replace("\u00a0", " ").strip()


def parse_month_from_title(title):
    title = clean_html(title)
    m = re.search(r"(\d{4})年(\d{1,2})月", title)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    return f"{year:04d}-{month:02d}"


def fetch_search_results():
    page = 1
    page_size = 50
    results = []
    while True:
        payload = {
            "qt": QUERY,
            "siteCode": SITE_CODE,
            "page": page,
            "pageSize": page_size,
        }
        data = post_json(API_URL, payload)
        docs = data.get("resultDocs") or []
        if not docs:
            break
        for doc in docs:
            d = doc.get("data", {})
            title = clean_html(d.get("title"))
            url = d.get("url")
            doc_date = d.get("docDate")
            if title and url:
                results.append({"title": title, "url": url, "docDate": doc_date})
        if len(docs) < page_size:
            break
        page += 1
    return results


def normalize_table(df):
    if df.shape[1] < 4:
        return None

    # Detect header offset (some tables include a title row)
    first_cell = clean_html(df.iloc[0, 0])
    header_offset = 0 if first_cell == "城市" else 1

    # Category tables (10 columns): combine two header rows to make unique names
    if df.shape[1] >= 10:
        header0 = [clean_html(x) for x in df.iloc[header_offset].tolist()]
        header1 = [clean_html(x) for x in df.iloc[header_offset + 1].tolist()]
        headers = []
        for i in range(len(header0)):
            if i == 0:
                headers.append("城市")
            else:
                h0 = header0[i].replace(" ", "")
                h1 = header1[i].replace(" ", "")
                h = f"{h0}-{h1}" if h1 and h0 else (h0 or h1)
                headers.append(h)
        data = df.iloc[header_offset + 3 :].copy()
        data.columns = headers
        merged = data
    else:
        # Simple tables: two side-by-side blocks
        left_header = [clean_html(x) for x in df.iloc[header_offset, 0:4].tolist()]
        if not left_header or left_header[0] == "":
            left_header = ["城市", "环比", "同比", "1-12月平均"]
        data = df.iloc[header_offset + 2 :].copy()
        left = data.iloc[:, 0:4]
        left.columns = left_header

        if df.shape[1] >= 8:
            right = data.iloc[:, 4:8]
            right.columns = left_header
            merged = pd.concat([left, right], axis=0, ignore_index=True)
        else:
            merged = left

    # Clean city names
    merged["城市"] = merged["城市"].astype(str).str.replace(" ", "", regex=False).str.strip()
    merged = merged[merged["城市"].notna() & (merged["城市"] != "")]
    return merged


def fetch_html(url, retries=3):
    last_err = None
    for _ in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as resp:
                return resp.read()
        except Exception as e:
            last_err = e
    raise last_err


def parse_tables(url, month):
    html = fetch_html(url)
    tables = pd.read_html(html)
    out = []
    for i, df in enumerate(tables[:6]):
        norm = normalize_table(df)
        if norm is None:
            continue
        norm.insert(0, "month", month)
        norm.insert(1, "table", TABLE_NAMES[i])
        out.append(norm)
    return out


def load_index():
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": {}}


def save_index(index):
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)


def save_processed(frames):
    if not frames:
        return
    all_df = pd.concat(frames, ignore_index=True)
    # Save per table
    for table_name in TABLE_NAMES:
        tdf = all_df[all_df["table"] == table_name]
        if tdf.empty:
            continue
        path = os.path.join(PROCESSED_DIR, f"{table_name}.csv")
        if os.path.exists(path):
            existing = pd.read_csv(path)
            merged = pd.concat([existing, tdf], ignore_index=True)
            merged = merged.drop_duplicates(subset=["month", "城市"], keep="last")
            merged.to_csv(path, index=False)
        else:
            tdf.to_csv(path, index=False)

    # Save all
    all_path = os.path.join(PROCESSED_DIR, "all.csv")
    if os.path.exists(all_path):
        existing = pd.read_csv(all_path)
        merged = pd.concat([existing, all_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["month", "table", "城市"], keep="last")
        merged.to_csv(all_path, index=False)
    else:
        all_df.to_csv(all_path, index=False)

    # also export json
    json_path = os.path.join(PROCESSED_DIR, "all.json")
    json_df = all_df.rename(columns={"城市": "city"}).copy()
    json_df.to_json(json_path, force_ascii=False, orient="records")


def main():
    index = load_index()
    processed = index.get("processed", {})

    results = fetch_search_results()
    # Filter relevant titles
    targets = [r for r in results if "70个大中城市" in r["title"] and "商品住宅销售价格变动" in r["title"]]
    failures = []

    for item in targets:
        month = parse_month_from_title(item["title"])
        if not month:
            continue
        if month in processed:
            continue
        url = item["url"]
        print(f"Processing {month} {url}")
        # Save raw HTML
        raw_path = os.path.join(RAW_DIR, f"{month}.html")
        if not os.path.exists(raw_path):
            try:
                html = fetch_html(url)
                with open(raw_path, "wb") as f:
                    f.write(html)
            except Exception:
                pass
        try:
            month_frames = parse_tables(url, month)
            save_processed(month_frames)
            processed[month] = {
                "title": item["title"],
                "url": url,
                "docDate": item.get("docDate"),
            }
            save_index({"processed": processed})
            print(f"Done {month}")
        except Exception as e:
            failures.append({"month": month, "url": url, "error": str(e)})
            print(f"Failed {month} {url}: {e}", file=sys.stderr)
            continue

    if failures:
        with open(os.path.join(PROCESSED_DIR, "failures.json"), "w", encoding="utf-8") as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)
        print(f"Failures: {len(failures)}")
    elif not targets:
        print("No new data")


if __name__ == "__main__":
    main()
