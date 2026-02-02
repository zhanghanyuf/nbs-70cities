#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil

BASE = os.path.dirname(__file__)
DATA_JSON = os.path.join(BASE, "..", "data", "processed", "all.json")
DOCS = os.path.join(BASE, "..", "docs")

os.makedirs(DOCS, exist_ok=True)
if os.path.exists(DATA_JSON):
    shutil.copyfile(DATA_JSON, os.path.join(DOCS, "data.json"))
    print("docs/data.json updated")
else:
    print("data/processed/all.json not found")
