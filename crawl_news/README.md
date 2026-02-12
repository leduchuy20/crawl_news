# 📰 Vietnamese News Data

Dữ liệu tin tức từ các báo lớn Việt Nam, tự động cập nhật mỗi 6 giờ.

## 🚀 Cách sử dụng

### Đọc trực tiếp từ URL với Python

```python
import pandas as pd

# Base URL
BASE_URL = "https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/"

# Đọc từng file
df_tonghop = pd.read_csv(BASE_URL + "rss_feed_articles_v2.csv")
df_laodong = pd.read_csv(BASE_URL + "laodong_html_articles_vi.csv")
df_znews = pd.read_csv(BASE_URL + "znews_html_categories_vi.csv")
df_24h = pd.read_csv(BASE_URL + "24h_html_categories_vi.csv")

print(f"Tổng hợp: {len(df_tonghop):,} bài")
print(f"Lao Động: {len(df_laodong):,} bài")
print(f"ZNews: {len(df_znews):,} bài")
print(f"24h: {len(df_24h):,} bài")
```

## 📊 Direct Download Links

| Nguồn | Link |
|-------|------|
| **Tổng hợp RSS** | [Download CSV](https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv) |
| **Lao Động** | [Download CSV](https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/laodong_html_articles_vi.csv) |
| **ZNews** | [Download CSV](https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/znews_html_categories_vi.csv) |
| **24h** | [Download CSV](https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/24h_html_categories_vi.csv) |

## 🔄 Cập nhật

Dữ liệu tự động cập nhật **mỗi 6 giờ** qua GitHub Actions.

## 📝 Cấu trúc dữ liệu

``` Columns:
- id              : Unique identifier (MD5 hash)
- title           : Tiêu đề bài viết
- published_at    : Thời gian xuất bản (ISO 8601 UTC)
- source.name     : Nguồn tin (VNExpress, Thanh Niên, Tuổi Trẻ, etc.)
- url             : Link bài viết gốc
- language        : Ngôn ngữ (vi)
- category.primary: Chuyên mục chính
- keywords        : Từ khóa (phân cách bởi |)
- entities        : Thực thể được trích xuất (phân cách bởi |)
- content.text    : Nội dung đầy đủ của bài viết
```

## 💡 Ví dụ nâng cao

### Merge tất cả nguồn

```python
import pandas as pd

BASE_URL = "https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/"

files = {
    'tonghop': 'rss_feed_articles_v2.csv',
    'laodong': 'laodong_html_articles_vi.csv',
    'znews': 'znews_html_categories_vi.csv',
    '24h': '24h_html_categories_vi.csv'
}

dfs = []
for source, filename in files.items():
    df = pd.read_csv(BASE_URL + filename)
    df['data_source'] = source
    dfs.append(df)
    print(f"✓ Loaded {len(df):,} articles from {source}")

# Merge tất cả
df_all = pd.concat(dfs, ignore_index=True)
df_all['published_at'] = pd.to_datetime(df_all['published_at'])

print(f"\n📊 Total: {len(df_all):,} articles")
```

### Phân tích từ khóa phổ biến

```python
import pandas as pd
from collections import Counter

df = pd.read_csv("https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv")

# Extract all keywords
all_keywords = []
for kw_str in df['keywords'].dropna():
    keywords = kw_str.split('|')
    all_keywords.extend(keywords)

# Count top keywords
keyword_counts = Counter(all_keywords)
print("Top 20 keywords:")
for keyword, count in keyword_counts.most_common(20):
    print(f"{keyword:<30} {count:>5,}")
```

### Phân tích xu hướng theo thời gian

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv")

# Convert to datetime
df['published_at'] = pd.to_datetime(df['published_at'])
df['date'] = df['published_at'].dt.date

# Count articles per day
daily_counts = df['date'].value_counts().sort_index()

# Plot
plt.figure(figsize=(12, 6))
daily_counts.plot(kind='line', marker='o')
plt.title('Số lượng bài viết theo ngày', fontsize=16)
plt.xlabel('Ngày')
plt.ylabel('Số bài viết')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

### Phân tích theo nguồn tin

```python
import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv")

# Count by source
source_counts = df['source.name'].value_counts()

print("Phân bố theo nguồn:")
for source, count in source_counts.items():
    percentage = count/len(df)*100
    print(f"{source:<30} {count:>6,} bài ({percentage:.1f}%)")
```

### Lọc bài viết theo từ khóa

```python
import pandas as pd

df = pd.read_csv("https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv")

# Tìm bài viết có chứa từ khóa "kinh tế"
keyword = "kinh tế"
filtered = df[df['keywords'].str.contains(keyword, case=False, na=False)]

print(f"Tìm thấy {len(filtered)} bài về '{keyword}':")
for idx, row in filtered.head(10).iterrows():
    print(f"\n- {row['title']}")
    print(f"  Nguồn: {row['source.name']} | {row['published_at']}")
    print(f"  Link: {row['url']}")
```

## 🔍 Google Colab Example

```python
# Chạy trong Google Colab
!pip install pandas matplotlib -q

import pandas as pd

# Load data
url = "https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/rss_feed_articles_v2.csv"
df = pd.read_csv(url)

# Quick stats
print(f"Total articles: {len(df):,}")
print(f"\nTop 10 sources:")
print(df['source.name'].value_counts().head(10))

print(f"\nTop 10 categories:")
print(df['category.primary'].value_counts().head(10))
```

## 🛠️ Technologies

- **Python**: Crawling & data processing
- **Pandas**: Data manipulation
- **GitHub Actions**: Automated scheduling
- **Docker**: Containerization (optional)

## 📂 Project Structure

```
crawl_news/
├── crawl/
│   ├── all_crawl.ipynb          # Main crawler notebook
│   ├── rss_feed_articles_v2.csv # Aggregated RSS data
│   ├── laodong_html_articles_vi.csv
│   ├── znews_html_categories_vi.csv
│   └── 24h_html_categories_vi.csv
├── crawl_news/                   # Individual crawler notebooks
├── dags/                         # Airflow DAGs (if using)
├── jobs/                         # Spark jobs (if using)
├── requirements.txt              # Python dependencies
└── docker-compose.yml            # Docker setup
```

## 📋 Requirements

```
pandas
feedparser
beautifulsoup4
requests
```

## 🔐 Privacy & Legal

- Dữ liệu được thu thập từ các nguồn công khai (RSS feeds)
- Chỉ sử dụng cho mục đích nghiên cứu và giáo dục
- Không sử dụng cho mục đích thương mại
- Tôn trọng bản quyền của các trang tin gốc

## 📜 License

MIT License - Free to use for research and education.

## 🤝 Contributing

Issues and Pull Requests are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Contact

- GitHub: [@leduchuy20](https://github.com/leduchuy20)
- Repository: [crawl_news](https://github.com/leduchuy20/crawl_news)

## 📈 Stats

![GitHub stars](https://img.shields.io/github/stars/leduchuy20/crawl_news?style=social)
![GitHub forks](https://img.shields.io/github/forks/leduchuy20/crawl_news?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/leduchuy20/crawl_news?style=social)

---

⭐ **Star this repo** if you find it useful!

**Last updated:** Auto-updated every 6 hours via GitHub Actions
