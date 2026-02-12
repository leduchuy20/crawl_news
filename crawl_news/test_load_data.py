"""
Test script để load Vietnamese News Data từ GitHub
"""

import pandas as pd

# Base URL đúng
BASE_URL = "https://raw.githubusercontent.com/leduchuy20/crawl_news/main/crawl_news/crawl/"

print("=" * 60)
print("📰 LOADING VIETNAMESE NEWS DATA FROM GITHUB")
print("=" * 60)

files = {
    'Tổng hợp RSS': 'rss_feed_articles_v2.csv',
    'Lao Động': 'laodong_html_articles_vi.csv',
    'ZNews': 'znews_html_categories_vi.csv',
    '24h': '24h_html_categories_vi.csv'
}

total_articles = 0

for source, filename in files.items():
    url = BASE_URL + filename
    print(f"\n📥 Loading {source}...", end=" ")
    
    try:
        df = pd.read_csv(url)
        count = len(df)
        total_articles += count
        print(f"✅ {count:,} articles")
        
        # Show sample
        print(f"   Latest: {df['title'].iloc[0][:60]}...")
        
    except Exception as e:
        print(f"❌ Failed: {e}")

print(f"\n{'=' * 60}")
print(f"📊 TOTAL: {total_articles:,} articles")
print(f"{'=' * 60}")

# Load one full dataset for demo
print(f"\n📖 Loading full dataset for analysis...")
df_all = pd.read_csv(BASE_URL + "rss_feed_articles_v2.csv")

print(f"\n✅ Loaded {len(df_all):,} articles")
print(f"\nColumns: {', '.join(df_all.columns.tolist())}")
print(f"\nTop 5 sources:")
print(df_all['source.name'].value_counts().head())

print(f"\n✨ Success! Data is ready to use!")
