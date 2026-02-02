#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil

import pandas as pd

BASE = os.path.dirname(__file__)
PROCESSED_DIR = os.path.join(BASE, "..", "data", "processed")
DOCS = os.path.join(BASE, "..", "docs")

os.makedirs(DOCS, exist_ok=True)
all_csv = os.path.join(PROCESSED_DIR, "all.csv")
all_json = os.path.join(PROCESSED_DIR, "all.json")

if os.path.exists(all_csv):
    df = pd.read_csv(all_csv)
    df.rename(columns={"城市": "city"}, inplace=True)
    df.to_json(all_json, force_ascii=False, orient="records")
    shutil.copyfile(all_json, os.path.join(DOCS, "data.json"))
    print("docs/data.json updated")
else:
    print("data/processed/all.csv not found")
