# 70城商品住宅销售价格变动数据

- 数据源：国家统计局（https://www.stats.gov.cn/sj/）
- 查询关键词：**70个大中城市商品住宅销售价格变动**

## 功能
- 自动检索并抓取历史与最新数据
- 解析 6 张表（新建/二手住宅及分类指数）
- 生成可筛选的网页（GitHub Pages）

## 目录结构
```
./scripts          # 抓取与构建脚本
./data/raw         # 原始 HTML
./data/processed   # 结构化数据（CSV/JSON）
./docs             # GitHub Pages 静态站点
```

## 使用
```bash
python3 -m pip install -r requirements.txt
python3 scripts/fetch_data.py
python3 scripts/build_web.py
```

## GitHub Pages
将 Pages Source 设置为 `docs/` 即可访问网页。
