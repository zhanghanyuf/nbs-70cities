#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil

BASE = os.path.dirname(__file__)
PROCESSED_DIR = os.path.join(BASE, "..", "data", "processed")
DOCS = os.path.join(BASE, "..", "docs")

os.makedirs(DOCS, exist_ok=True)
all_csv = os.path.join(PROCESSED_DIR, "all.csv")

if os.path.exists(all_csv):
    shutil.copyfile(all_csv, os.path.join(DOCS, "data.csv"))
    print("docs/data.csv updated")
else:
    print("data/processed/all.csv not found")
