# Big Data - News Crawler

Hệ thống crawl tin tức từ các báo Việt Nam.

## Setup Local

```bash
# Cài đặt dependencies
pip install -r requirements.txt

# Chạy crawler
cd crawl_news/crawl
jupyter notebook all_crawl.ipynb
```

Hoặc chạy bằng script Python:

```bash
cd crawl_news/crawl
python all_crawl.py
```

## Cấu trúc thư mục

```text
├── crawl_news/           # Mã nguồn crawl
│   └── crawl/
│       ├── all_crawl.ipynb
│       └── *.csv
├── dags/                 # Airflow DAGs (nếu có)
├── jobs/                 # Spark jobs
├── requirements.txt      # Python dependencies
└── docker-compose.yml    # Docker config
```
