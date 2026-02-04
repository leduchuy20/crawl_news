# Big Data - News Crawler

Hệ thống crawl tin tức tự động từ các báo Việt Nam.

## Tự động chạy hàng ngày

Dự án này sử dụng GitHub Actions để tự động crawl tin tức **mỗi ngày lúc 8h sáng** (giờ Việt Nam).

### Cấu hình

1. **GitHub Actions workflow**: `.github/workflows/daily-crawl.yml`
   - Tự động chạy mỗi ngày lúc 8h sáng VN (1h UTC)
   - Có thể chạy thủ công từ GitHub Actions UI

2. **Thay đổi giờ chạy**:
   
   Mở file `.github/workflows/daily-crawl.yml` và sửa dòng cron:
   ```yaml
   schedule:
     - cron: '0 1 * * *'  # Giờ theo UTC
   ```
   
   Ví dụ:
   - 8h sáng VN = `'0 1 * * *'` (1h UTC)
   - 12h trưa VN = `'0 5 * * *'` (5h UTC)
   - 6h chiều VN = `'0 11 * * *'` (11h UTC)
   - Chạy 2 lần/ngày (8h sáng và 6h chiều): 
     ```yaml
     schedule:
       - cron: '0 1 * * *'
       - cron: '0 11 * * *'
     ```

3. **Chạy thủ công**:
   
   Truy cập: `https://github.com/YOUR_USERNAME/YOUR_REPO/actions`
   - Chọn workflow "Daily News Crawl"
   - Click "Run workflow"

### Setup Local

```bash
# Cài đặt dependencies
pip install -r requirements.txt

# Chạy crawler
cd crawl_news/crawl
jupyter notebook all_crawl.ipynb
```

## Cấu trúc thư mục

```
├── .github/workflows/     # GitHub Actions workflows
├── crawl_news/           # Mã nguồn crawl
│   └── crawl/
│       ├── all_crawl.ipynb
│       └── *.csv
├── dags/                 # Airflow DAGs (nếu có)
├── jobs/                 # Spark jobs
├── requirements.txt      # Python dependencies
└── docker-compose.yml    # Docker config
```

## Lưu ý

- Kết quả crawl sẽ tự động commit và push về GitHub
- Kiểm tra tab "Actions" trên GitHub để xem log chạy
- Nếu có lỗi, GitHub sẽ gửi email thông báo
