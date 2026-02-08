#!/usr/bin/env python
# coding: utf-8

# # tong hop

# In[1]:


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import re
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VN_TZ = timezone(timedelta(hours=7))

# ================== CONFIG ==================
# Danh sách các RSS feeds từ các báo Việt Nam
RSS_FEEDS = [
    # VnExpress RSS (15 feeds)
    "https://vnexpress.net/rss/tin-moi-nhat.rss",
    "https://vnexpress.net/rss/thoi-su.rss",
    "https://vnexpress.net/rss/the-gioi.rss",
    "https://vnexpress.net/rss/kinh-doanh.rss",
    "https://vnexpress.net/rss/giai-tri.rss",
    "https://vnexpress.net/rss/the-thao.rss",
    "https://vnexpress.net/rss/phap-luat.rss",
    "https://vnexpress.net/rss/giao-duc.rss",
    "https://vnexpress.net/rss/suc-khoe.rss",
    "https://vnexpress.net/rss/gia-dinh.rss",
    "https://vnexpress.net/rss/du-lich.rss",
    "https://vnexpress.net/rss/khoa-hoc.rss",
    "https://vnexpress.net/rss/so-hoa.rss",
    "https://vnexpress.net/rss/oto-xe-may.rss",
    "https://vnexpress.net/rss/y-kien.rss",
    
    # Dân Trí RSS (10 feeds)
    "https://dantri.com.vn/rss/trang-chu.rss",
    "https://dantri.com.vn/rss/xa-hoi.rss",
    "https://dantri.com.vn/rss/the-gioi.rss",
    "https://dantri.com.vn/rss/kinh-doanh.rss",
    "https://dantri.com.vn/rss/the-thao.rss",
    "https://dantri.com.vn/rss/giai-tri.rss",
    "https://dantri.com.vn/rss/giao-duc.rss",
    "https://dantri.com.vn/rss/suc-khoe.rss",
    "https://dantri.com.vn/rss/du-lich.rss",
    "https://dantri.com.vn/rss/o-to-xe-may.rss",
    
    # Tuổi Trẻ RSS (9 feeds)
    "https://tuoitre.vn/rss/tin-moi-nhat.rss",
    "https://tuoitre.vn/rss/thoi-su.rss",
    "https://tuoitre.vn/rss/the-gioi.rss",
    "https://tuoitre.vn/rss/phap-luat.rss",
    "https://tuoitre.vn/rss/kinh-doanh.rss",
    "https://tuoitre.vn/rss/giao-duc.rss",
    "https://tuoitre.vn/rss/the-thao.rss",
    "https://tuoitre.vn/rss/giai-tri.rss",
    "https://tuoitre.vn/rss/xe.rss",
    
    # Thanh Niên RSS (8 feeds)
    "https://thanhnien.vn/rss/home.rss",
    "https://thanhnien.vn/rss/thoi-su.rss",
    "https://thanhnien.vn/rss/the-gioi.rss",
    "https://thanhnien.vn/rss/kinh-te.rss",
    "https://thanhnien.vn/rss/van-hoa.rss",
    "https://thanhnien.vn/rss/the-thao.rss",
    "https://thanhnien.vn/rss/cong-nghe.rss",
    "https://thanhnien.vn/rss/gioi-tre.rss",
    
    # VietnamNet RSS (5 feeds)
    "https://vietnamnet.vn/rss/thoi-su.rss",
    "https://vietnamnet.vn/rss/the-gioi.rss",
    "https://vietnamnet.vn/rss/kinh-doanh.rss",
    "https://vietnamnet.vn/rss/giao-duc.rss",
    "https://vietnamnet.vn/rss/the-thao.rss",
    
    # Lao Động RSS (11 feeds)
    "https://laodong.vn/rss/home.rss",
    "https://laodong.vn/rss/cong-doan.rss",
    "https://laodong.vn/rss/xa-hoi.rss",
    "https://laodong.vn/rss/kinh-doanh.rss",
    "https://laodong.vn/rss/van-hoa-giai-tri.rss",
    "https://laodong.vn/rss/xe.rss",
    "https://laodong.vn/rss/thoi-su.rss",
    "https://laodong.vn/rss/the-gioi.rss",
    "https://laodong.vn/rss/phap-luat.rss",
    "https://laodong.vn/rss/the-thao.rss",
    "https://laodong.vn/rss/suc-khoe.rss",
    
    # Người Lao Động RSS (17 feeds)
    "https://nld.com.vn/rss/home.rss",
    "https://nld.com.vn/rss/thoi-su.rss",
    "https://nld.com.vn/rss/quoc-te.rss",
    "https://nld.com.vn/rss/lao-dong.rss",
    "https://nld.com.vn/rss/ban-doc.rss",
    "https://nld.com.vn/rss/net-zero.rss",
    "https://nld.com.vn/rss/kinh-te.rss",
    "https://nld.com.vn/rss/suc-khoe.rss",
    "https://nld.com.vn/rss/giao-duc-khoa-hoc.rss",
    "https://nld.com.vn/rss/phap-luat.rss",
    "https://nld.com.vn/rss/van-hoa-van-nghe.rss",
    "https://nld.com.vn/rss/giai-tri.rss",
    "https://nld.com.vn/rss/the-thao.rss",
    "https://nld.com.vn/rss/ai-365.rss",
    "https://nld.com.vn/rss/du-lich-xanh.rss",
    "https://nld.com.vn/rss/khoa-hoc.rss",
    "https://nld.com.vn/rss/nguoi-lao-dong-news.rss",
    
    # VietnamPlus RSS (19 feeds)
    "https://www.vietnamplus.vn/rss/home.rss",
    "https://www.vietnamplus.vn/rss/chinhtri-291.rss",
    "https://www.vietnamplus.vn/rss/thegioi-209.rss",
    "https://www.vietnamplus.vn/rss/thegioi/asean-356.rss",
    "https://www.vietnamplus.vn/rss/thegioi/chaua-tbd-352.rss",
    "https://www.vietnamplus.vn/rss/thegioi/trungdong-230.rss",
    "https://www.vietnamplus.vn/rss/thegioi/chauau-354.rss",
    "https://www.vietnamplus.vn/rss/thegioi/chauphi-357.rss",
    "https://www.vietnamplus.vn/rss/thegioi/chaumy-355.rss",
    "https://www.vietnamplus.vn/rss/kinhte-311.rss",
    "https://www.vietnamplus.vn/rss/kinhte/kinhdoanh-342.rss",
    "https://www.vietnamplus.vn/rss/kinhte/taichinh-343.rss",
    "https://www.vietnamplus.vn/rss/xahoi-314.rss",
    "https://www.vietnamplus.vn/rss/xahoi/giaoduc-316.rss",
    "https://www.vietnamplus.vn/rss/xahoi/yte-325.rss",
    "https://www.vietnamplus.vn/rss/xahoi/phapluat-327.rss",
    "https://www.vietnamplus.vn/rss/xahoi/giaothong-358.rss",
    "https://www.vietnamplus.vn/rss/doisong-320.rss",
    "https://www.vietnamplus.vn/rss/thethao-214.rss",
    
    # Soha RSS (9 feeds)
    "https://soha.vn/rss/home.rss",
    "https://soha.vn/rss/thoi-su-xa-hoi.rss",
    "https://soha.vn/rss/kinh-doanh.rss",
    "https://soha.vn/rss/quoc-te.rss",
    "https://soha.vn/rss/the-thao.rss",
    "https://soha.vn/rss/giai-tri.rss",
    "https://soha.vn/rss/phap-luat.rss",
    "https://soha.vn/rss/viet-nam-vuon-minh.rss",
    "https://soha.vn/rss/sea-games-32.rss",
    
    # Nhân Dân RSS (16 feeds)
    "https://nhandan.vn/rss/home.rss",
    "https://nhandan.vn/rss/chinhtri-1171.rss",
    "https://nhandan.vn/rss/xa-luan-1176.rss",
    "https://nhandan.vn/rss/xay-dung-dang-1179.rss",
    "https://nhandan.vn/rss/kinhte-1185.rss",
    "https://nhandan.vn/rss/chungkhoan-1191.rss",
    "https://nhandan.vn/rss/phapluat-1287.rss",
    "https://nhandan.vn/rss/du-lich-1257.rss",
    "https://nhandan.vn/rss/thegioi-1231.rss",
    "https://nhandan.vn/rss/asean-704471.rss",
    "https://nhandan.vn/rss/chau-phi-704476.rss",
    "https://nhandan.vn/rss/chau-my-704475.rss",
    "https://nhandan.vn/rss/chau-au-704474.rss",
    "https://nhandan.vn/rss/trung-dong-704473.rss",
    "https://nhandan.vn/rss/chau-a-tbd-704472.rss",
    "https://nhandan.vn/rss/thethao-1224.rss",
    
    # Báo Tin Tức RSS (10 feeds)
    "https://baotintuc.vn/tin-moi-nhat.rss",
    "https://baotintuc.vn/thoi-su.rss",
    "https://baotintuc.vn/the-gioi.rss",
    "https://baotintuc.vn/kinh-te.rss",
    "https://baotintuc.vn/xa-hoi.rss",
    "https://baotintuc.vn/phap-luat.rss",
    "https://baotintuc.vn/giao-duc.rss",
    "https://baotintuc.vn/van-hoa.rss",
    "https://baotintuc.vn/the-thao.rss",
    "https://baotintuc.vn/quan-su.rss",
    
    # Kiến Thức RSS (8 feeds)
    "https://kienthuc.net.vn/rss/home.rss",
    "https://kienthuc.net.vn/rss/nha-khoa-hoc-345.rss",
    "https://kienthuc.net.vn/rss/spotlight-379.rss",
    "https://kienthuc.net.vn/rss/chinh-tri-348.rss",
    "https://kienthuc.net.vn/rss/xa-hoi-349.rss",
    "https://kienthuc.net.vn/rss/the-gioi-350.rss",
    "https://kienthuc.net.vn/rss/quan-su-359.rss",
    "https://kienthuc.net.vn/rss/giai-tri-365.rss",
]

# Crawl tất cả bài viết từ RSS feeds
END_DATE = "2026-01-15"  # YYYY-MM-DD - chỉ lấy bài >= ngày này

CSV_PATH = "rss_feed_articles_v2.csv"

# Có fetch full content từ URL gốc không (chậm hơn nhưng đầy đủ hơn)
FETCH_FULL_CONTENT = True

TIMEOUT = 25
REQUEST_DELAY_BASE = 0.25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RSSCrawler/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}
# ===========================================

CSV_HEADER = [
    "id",
    "title",
    "published_at",        # ISO UTC
    "source.name",
    "url",
    "language",
    "category.primary",
    "keywords",
    "entities",
    "content.text",
]

SOURCE_NAME = "RSS_Feed"
DEFAULT_LANGUAGE = "vi"
DEBUG = False

# ----- HTTP session with retry -----
session = requests.Session()
session.headers.update(HEADERS)

retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
    respect_retry_after_header=True,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)


def log(msg: str):
    if DEBUG:
        print(msg)


def polite_sleep():
    time.sleep(REQUEST_DELAY_BASE)


def md5_id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def fetch_text(url: str) -> str:
    r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text


def ensure_csv_header(csv_path: str):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)


def load_seen_from_csv(csv_path: str) -> Tuple[Set[str], Set[str]]:
    seen_urls, seen_ids = set(), set()
    if not os.path.exists(csv_path):
        return seen_urls, seen_ids
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, None)
            if not header:
                return seen_urls, seen_ids
            id_idx = header.index("id") if "id" in header else 0
            url_idx = header.index("url") if "url" in header else 4
            for row in r:
                if len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u:
                        seen_urls.add(u)
                if len(row) > id_idx:
                    i = row[id_idx].strip()
                    if i:
                        seen_ids.add(i)
    except Exception:
        pass
    return seen_urls, seen_ids


def append_row(csv_path: str, row: Dict[str, Any]):
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([row.get(k, "") for k in CSV_HEADER])
        f.flush()


def iso_to_local_date(iso_utc: str) -> Optional[str]:
    if not iso_utc:
        return None
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(VN_TZ).date().isoformat()
    except Exception:
        return None


def parse_rss_date(date_str: str) -> Optional[str]:
    """
    Parse RSS date format to ISO UTC
    RSS thường dùng RFC 2822 hoặc ISO format
    """
    if not date_str:
        return None
    
    try:
        # feedparser tự động parse date
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    
    # Thử ISO format
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=VN_TZ)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    
    return None


def extract_category_from_url(url: str) -> Optional[str]:
    """Trích xuất category từ URL pattern"""
    from urllib.parse import urlparse
    
    # Mapping các patterns phổ biến
    category_map = {
        'thoi-su': 'Thời sự',
        'the-gioi': 'Thế giới', 
        'xa-hoi': 'Xã hội',
        'kinh-doanh': 'Kinh doanh',
        'giai-tri': 'Giải trí',
        'the-thao': 'Thể thao',
        'phap-luat': 'Pháp luật',
        'giao-duc': 'Giáo dục',
        'suc-khoe': 'Sức khỏe',
        'gia-dinh': 'Gia đình',
        'du-lich': 'Du lịch',
        'khoa-hoc': 'Khoa học',
        'so-hoa': 'Số hóa',
        'cong-nghe': 'Công nghệ',
        'oto-xe-may': 'Ôtô-Xe máy',
        'doi-song': 'Đời sống',
        'van-hoa': 'Văn hóa',
        'tin-tuc': 'Tin tức',
        'video': 'Video',
    }
    
    try:
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        
        # Tìm category trong các phần của URL (chỉ xét 2 phần đầu)
        for part in path_parts[:2]:
            for pattern, category in category_map.items():
                if pattern in part:
                    return category
    except Exception:
        pass
    
    return None


def extract_keywords_from_entry(entry: Any, url: str) -> List[str]:
    """Trích xuất keywords từ RSS entry và URL"""
    keywords = []
    
    # 1. Lấy từ tags RSS (Dân Trí, Tuổi Trẻ có field này)
    if 'tags' in entry and entry.tags:
        for tag in entry.tags:
            term = tag.get('term', '').strip()
            if term and term not in keywords:
                keywords.append(term)
    
    # 2. Lấy từ category field (nếu không có trong tags)
    if 'category' in entry and entry.category:
        cat = entry.category.strip()
        if cat and cat not in keywords:
            keywords.append(cat)
    
    # 3. Trích xuất từ URL
    url_category = extract_category_from_url(url)
    if url_category and url_category not in keywords:
        keywords.append(url_category)
    
    return keywords


def extract_content_from_html(html_content: str) -> str:
    """Trích xuất text từ HTML content trong RSS"""
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, "lxml")
        # Lấy tất cả text, loại bỏ tags
        text = soup.get_text(separator=" ", strip=True)
        # Làm sạch whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception:
        return html_content


def fetch_article_content(url: str) -> str:
    """
    Fetch nội dung đầy đủ từ URL bài viết
    Nếu RSS chỉ có summary, cần fetch trang gốc
    """
    try:
        html = fetch_text(url)
        soup = BeautifulSoup(html, "lxml")
        
        # Thử các selector phổ biến
        article_body = None
        selectors = [
            "article",
            ".article-content",
            ".post-content",
            ".entry-content",
            ".content",
            "main",
        ]
        
        for selector in selectors:
            article_body = soup.select_one(selector)
            if article_body:
                break
        
        if article_body:
            paragraphs = article_body.find_all("p")
            text_parts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    text_parts.append(text)
            return " ".join(text_parts)
        
        return ""
    except Exception as e:
        log(f"Failed to fetch article content from {url}: {e}")
        return ""


def parse_rss_entry(entry: Any, seen_urls: Set[str], fetch_full_content: bool = False) -> Optional[Dict[str, Any]]:
    """Parse một entry từ RSS feed"""
    
    # URL
    url = entry.get("link", "").strip()
    if not url or url in seen_urls:
        return None
    
    # Title
    title = entry.get("title", "").strip()
    
    # Published date
    pub = ""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            pub = dt.isoformat()
        except Exception:
            pass
    
    if not pub and "published" in entry:
        pub = parse_rss_date(entry.published) or ""
    
    if not pub and "pubDate" in entry:
        pub = parse_rss_date(entry.pubDate) or ""
    
    # Content
    content_text = ""
    
    # Thử lấy content từ RSS
    if "content" in entry and entry.content:
        # feedparser trả về list
        content_html = entry.content[0].get("value", "") if isinstance(entry.content, list) else entry.content
        content_text = extract_content_from_html(content_html)
    
    # Nếu không có content, thử summary/description
    if not content_text:
        if "summary" in entry:
            content_text = extract_content_from_html(entry.summary)
        elif "description" in entry:
            content_text = extract_content_from_html(entry.description)
    
    # Nếu cần fetch full content từ URL gốc
    if fetch_full_content and url:
        full_content = fetch_article_content(url)
        if full_content and len(full_content) > len(content_text):
            content_text = full_content
    
    # Keywords - tự động trích xuất từ tags RSS hoặc URL
    keywords = extract_keywords_from_entry(entry, url)
    
    # Category - ưu tiên từ tags RSS, sau đó từ URL
    category = ""
    if keywords:
        category = keywords[0]  # Lấy keyword đầu tiên làm primary category
    
    # Author có thể là source
    author = entry.get("author", "")
    
    return {
        "url": url,
        "title": title,
        "published_at": pub,
        "content_text": content_text,
        "category": category,
        "keywords": keywords,
        "author": author,
    }


def make_row(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": md5_id(data["url"]),
        "title": data.get("title") or "",
        "published_at": data.get("published_at") or "",
        "source.name": data.get("author") or SOURCE_NAME,
        "url": data["url"],
        "language": DEFAULT_LANGUAGE,
        "category.primary": data.get("category") or "",
        "keywords": "|".join(data.get("keywords") or []),
        "entities": "",
        "content.text": data.get("content_text") or "",
    }


def crawl_rss_feed(feed_url: str, end_date: str, seen_urls: Set[str], seen_ids: Set[str], fetch_full_content: bool = False) -> int:
    """
    Crawl RSS feed
    fetch_full_content: True nếu muốn fetch nội dung đầy đủ từ URL gốc
    """
    added = 0
    skipped_old = 0
    skipped_duplicate = 0
    
    try:
        # Parse RSS feed
        print(f"Fetching RSS feed: {feed_url}")
        feed = feedparser.parse(feed_url)
        
        if not feed.entries:
            print("No entries found in RSS feed")
            return 0
        
        print(f"Found {len(feed.entries)} entries in RSS feed")
        
        # Parse end_date
        end_dt = datetime.fromisoformat(end_date).replace(tzinfo=VN_TZ)
        
        for entry in feed.entries:
            try:
                # Parse entry
                data = parse_rss_entry(entry, seen_urls, fetch_full_content=False)
                if not data:
                    skipped_duplicate += 1
                    continue
                
                # Filter by date
                pub_iso = data.get("published_at")
                if pub_iso:
                    pub_local_date = iso_to_local_date(pub_iso)
                    if pub_local_date and pub_local_date < end_date:
                        skipped_old += 1
                        continue
                
                # Check duplicate by ID
                aid = md5_id(data["url"])
                if aid in seen_ids:
                    skipped_duplicate += 1
                    continue
                
                # Fetch full content if needed
                if fetch_full_content:
                    full_content = fetch_article_content(data["url"])
                    if full_content and len(full_content) > len(data.get("content_text", "")):
                        data["content_text"] = full_content
                
                # Ghi bài vào CSV
                row = make_row(data)
                append_row(CSV_PATH, row)
                seen_urls.add(data["url"])
                seen_ids.add(aid)
                added += 1
                
                print(f"Added: {data.get('title', '')[:80]}")
                
                if fetch_full_content:
                    polite_sleep()
                    
            except Exception as e:
                log(f"Error processing entry: {e}")
                continue
        
        # Summary log
        print(f"\nSummary: {added} added, {skipped_duplicate} duplicates, {skipped_old} old")
        return added
        
    except Exception as e:
        print(f"Error crawling RSS feed: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    ensure_csv_header(CSV_PATH)
    seen_urls, seen_ids = load_seen_from_csv(CSV_PATH)
    
    total_added = 0
    
    for i, feed_url in enumerate(RSS_FEEDS, 1):
        print(f"\n{'='*80}")
        print(f"Processing RSS Feed {i}/{len(RSS_FEEDS)}")
        print(f"{'='*80}")
        
        try:
            added = crawl_rss_feed(feed_url, END_DATE, seen_urls, seen_ids, fetch_full_content=FETCH_FULL_CONTENT)
            total_added += added
            print(f"✓ Added {added} articles from this feed")
        except Exception as e:
            print(f"✗ Error with feed {feed_url}: {e}")
            continue
        
        # Delay giữa các feeds
        if i < len(RSS_FEEDS):
            time.sleep(2)
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total feeds processed: {len(RSS_FEEDS)}")
    print(f"Total articles added: {total_added}")
    print(f"Output file: {CSV_PATH}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

# In[4]:


# Kiểm tra kết quả sau khi crawl 137 RSS feeds
import pandas as pd
import os
df = pd.read_csv("rss_feed_articles_v2.csv")

print("=" * 80)
print("KẾT QUẢ CRAWL VỚI 137 RSS FEEDS")
print("=" * 80)

print(f"\n📊 TỔNG QUAN:")
print(f"   - Tổng số bài viết: {len(df):,}")
print(f"   - Số feeds: 137 (từ 12 nguồn tin)")

print(f"\n📰 PHÂN BỐ THEO NGUỒN (Top 20):")
source_dist = df['source.name'].value_counts().head(20)
for source, count in source_dist.items():
    print(f"   {source:<30} {count:>5,} bài")

print(f"\n📅 PHÂN BỐ THEO NGÀY:")
df['date'] = pd.to_datetime(df['published_at']).dt.date
date_dist = df['date'].value_counts().sort_index(ascending=False).head(7)
for date, count in date_dist.items():
    print(f"   {date}: {count:>4,} bài")

print(f"\n🏷️ KEYWORDS & CATEGORY:")
keywords_count = df['keywords'].notna().sum()
category_count = df['category.primary'].notna().sum()
print(f"   - Số bài có keywords: {keywords_count:,}/{len(df):,} ({keywords_count/len(df)*100:.1f}%)")
print(f"   - Số bài có category: {category_count:,}/{len(df):,} ({category_count/len(df)*100:.1f}%)")

print(f"\n📂 TOP 20 CATEGORIES:")
cat_dist = df['category.primary'].value_counts().head(20)
for cat, count in cat_dist.items():
    print(f"   {cat:<25} {count:>4,} bài")

# Tính độ phủ dữ liệu
avg_content_length = df['content.text'].str.len().mean()
print(f"\n📝 CHẤT LƯỢNG DỮ LIỆU:")
print(f"   - Độ dài trung bình content: {avg_content_length:,.0f} ký tự")
print(f"   - Số bài có content: {df['content.text'].notna().sum():,}/{len(df):,}")

print("\n" + "=" * 80)
print(f"✅ HOÀN TẤT! File: rss_feed_articles_v2.csv")
file_size_mb = os.path.getsize('rss_feed_articles_v2.csv') / 1024 / 1024
print(f"   Dung lượng: {file_size_mb:.2f} MB")
print(f"   Nguồn tin: 12 tờ báo lớn nhất Việt Nam")
print("=" * 80)

# # lao dong

# In[ ]:


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# LaoDong HTML Crawler - Crawl theo category pages với pagination

import csv
import os
import re
import time
import random
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
from urllib.parse import urlparse, urlencode

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VN_TZ = timezone(timedelta(hours=7))

# ================== CONFIG ==================
CATEGORY_URLS = [
    "https://laodong.vn/thoi-su/",
    "https://laodong.vn/the-gioi/",
    "https://laodong.vn/xa-hoi/",
    "https://laodong.vn/phap-luat/",
    "https://laodong.vn/kinh-doanh/",
    "https://laodong.vn/bat-dong-san/",
    "https://laodong.vn/van-hoa/",
    "https://laodong.vn/giao-duc/",
    "https://laodong.vn/the-thao/",
    "https://laodong.vn/suc-khoe/",
    "https://laodong.vn/cong-nghe/",
    "https://laodong.vn/xe/",
    "https://laodong.vn/du-lich/",
]

# Crawl từ mới -> cũ cho tới khi bài có ngày < END_DATE
END_DATE = "2026-01-15"  # YYYY-MM-DD - Lấy từ tháng 12/2025 để có nhiều dữ liệu hơn
MAX_PAGES_PER_CATEGORY = 100  # Mỗi category tối đa 100 trang

CSV_PATH = "laodong_html_articles_vi.csv"

TIMEOUT = 25
REQUEST_DELAY_BASE = 0.3
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
# ===========================================

CSV_HEADER = [
    "id",
    "title",
    "published_at",
    "source.name",
    "url",
    "language",
    "category.primary",
    "keywords",
    "entities",
    "content.text",
]

SOURCE_NAME = "LaoDong"
DEFAULT_LANGUAGE = "vi"
DEBUG = False

# ----- HTTP session with retry -----
session = requests.Session()
session.headers.update(HEADERS)

retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
    respect_retry_after_header=True,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)


def log(msg: str):
    if DEBUG:
        print(msg)


def polite_sleep():
    time.sleep(REQUEST_DELAY_BASE + random.uniform(0, 0.4))


def md5_id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def fetch_with_cookie_handling(url: str) -> requests.Response:
    """Fetch URL with laodong.vn cookie protection handling"""
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    
    # Check if response is cookie-setting JavaScript
    if "document.cookie" in r.text and len(r.content) < 500:
        match = re.search(r'document\.cookie="([^"]+)"', r.text)
        if match:
            cookie_str = match.group(1)
            cookie_parts = cookie_str.split("=", 1)
            if len(cookie_parts) == 2:
                cookie_name, cookie_value = cookie_parts
                session.cookies.set(cookie_name, cookie_value)
                log(f"[DEBUG] Set cookie: {cookie_name}={cookie_value[:20]}...")
        
        polite_sleep()
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    
    return r


def fetch_text(url: str) -> str:
    return fetch_with_cookie_handling(url).text


def to_iso_utc(s: Optional[str]) -> Optional[str]:
    """Convert datetime string to ISO UTC format"""
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=VN_TZ)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def iso_to_local_date(iso_utc: str) -> Optional[str]:
    """Convert ISO UTC to local date YYYY-MM-DD"""
    if not iso_utc:
        return None
    try:
        dt = dateparser.parse(iso_utc)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_local = dt.astimezone(VN_TZ)
        return dt_local.date().isoformat()
    except Exception:
        return None


def ensure_csv_header(csv_path: str):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)


def load_seen_from_csv(csv_path: str) -> Tuple[Set[str], Set[str]]:
    seen_urls, seen_ids = set(), set()
    if not os.path.exists(csv_path):
        return seen_urls, seen_ids
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, None)
            if not header:
                return seen_urls, seen_ids
            id_idx = header.index("id") if "id" in header else 0
            url_idx = header.index("url") if "url" in header else 4
            for row in r:
                if len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u:
                        seen_urls.add(u)
                if len(row) > id_idx:
                    i = row[id_idx].strip()
                    if i:
                        seen_ids.add(i)
    except Exception:
        pass
    return seen_urls, seen_ids


def append_row(csv_path: str, row: Dict[str, Any]):
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([row.get(k, "") for k in CSV_HEADER])
        f.flush()


def extract_article_meta(article_html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(article_html, "lxml")

    # title
    title = ""
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        title = og["content"].strip()
    if not title:
        h1 = soup.select_one("h1")
        if h1:
            title = h1.get_text(strip=True)

    # published_at
    pub = ""
    m_pub = soup.select_one('meta[property="article:published_time"]')
    if m_pub and m_pub.get("content"):
        pub = to_iso_utc(m_pub["content"].strip()) or ""
    if not pub:
        m2 = soup.select_one('meta[itemprop="datePublished"]')
        if m2 and m2.get("content"):
            pub = to_iso_utc(m2["content"].strip()) or ""
    if not pub:
        ttag = soup.select_one("time")
        if ttag:
            pub = to_iso_utc(ttag.get("datetime") or ttag.get_text(strip=True)) or ""

    # category
    category_primary = ""
    sec = soup.select_one('meta[property="article:section"]')
    if sec and sec.get("content"):
        category_primary = sec["content"].strip()

    # language
    language = DEFAULT_LANGUAGE
    html_tag = soup.find("html")
    if html_tag:
        lang = html_tag.get("lang")
        if lang:
            language = lang.lower().strip()

    # keywords
    keywords = []
    kw = soup.select_one('meta[name="keywords"]')
    if kw and kw.get("content"):
        keywords = [x.strip() for x in kw["content"].split(",") if x.strip()]

    # content.text
    content_text = ""
    article_body = soup.select_one("article.detail-content")
    if not article_body:
        article_body = soup.select_one(".detail-content")
    if not article_body:
        article_body = soup.select_one("article")
    
    if article_body:
        paragraphs = article_body.find_all("p")
        text_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text:
                text_parts.append(text)
        content_text = " ".join(text_parts)

    return {
        "title": title,
        "published_at": pub,
        "language": language,
        "keywords": keywords,
        "category_from_article": category_primary,
        "entities": [],
        "content_text": content_text,
    }


def extract_article_urls_from_page(html: str, category_url: str) -> List[str]:
    """Extract article URLs from category page"""
    soup = BeautifulSoup(html, "lxml")
    urls = []
    
    # LaoDong uses various link patterns
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        
        # Make absolute URL
        if href.startswith("/"):
            href = "https://laodong.vn" + href
        
        # Only laodong.vn articles
        if not href.startswith("https://laodong.vn/"):
            continue
        
        # Article URLs end with .ldo
        if not href.endswith(".ldo"):
            continue
        
        # Remove query params
        href = href.split("?")[0]
        urls.append(href)
    
    # Remove duplicates
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    
    return result


def get_next_page_url(category_url: str, page: int) -> str:
    """
    LaoDong pagination: ?page=2, ?page=3, etc.
    """
    base_url = category_url.rstrip("/")
    return f"{base_url}?page={page}"


def make_row(url: str, meta: Dict[str, Any], category_fallback: str) -> Dict[str, Any]:
    return {
        "id": md5_id(url),
        "title": meta.get("title") or "",
        "published_at": meta.get("published_at") or "",
        "source.name": SOURCE_NAME,
        "url": url,
        "language": meta.get("language") or DEFAULT_LANGUAGE,
        "category.primary": (meta.get("category_from_article") or category_fallback) or "",
        "keywords": "|".join(meta.get("keywords") or []),
        "entities": "|".join(meta.get("entities") or []),
        "content.text": meta.get("content_text") or "",
    }


def category_slug_from_url(url: str) -> str:
    """Extract category slug from URL"""
    path = urlparse(url).path.strip("/")
    return path.split("/")[0] if "/" in path else path


def crawl_category(category_url: str, end_date: str, seen_urls: Set[str], seen_ids: Set[str]) -> Tuple[int, int, int]:
    """
    Crawl one category from new to old until < end_date
    Returns: (added, skipped_duplicate, skipped_old)
    """
    added = 0
    skipped_duplicate = 0
    skipped_old = 0
    page = 1
    category_slug = category_slug_from_url(category_url)
    
    print(f"\n[{category_slug}] Starting crawl...")
    
    while page <= MAX_PAGES_PER_CATEGORY:
        # Page 1 is the category URL, page 2+ use ?page=N
        if page == 1:
            url_page = category_url
        else:
            url_page = get_next_page_url(category_url, page)
        
        print(f"[{category_slug}] Fetching page {page}: {url_page}")
        
        try:
            html = fetch_text(url_page)
        except Exception as e:
            print(f"[{category_slug}] Page {page} fetch failed: {e}")
            break
        
        article_urls = extract_article_urls_from_page(html, category_url)
        
        if not article_urls:
            print(f"[{category_slug}] Page {page}: No articles found, stopping")
            break
        
        print(f"[{category_slug}] Page {page}: Found {len(article_urls)} candidate articles")
        
        page_all_older_than_end = True
        page_added = 0
        
        for aurl in article_urls:
            if aurl in seen_urls:
                continue
            
            aid = md5_id(aurl)
            if aid in seen_ids:
                continue
            
            # Fetch article
            try:
                ah = fetch_text(aurl)
                meta = extract_article_meta(ah)
            except Exception as e:
                log(f"[WARN] Article fetch failed {aurl}: {e}")
                continue
            finally:
                polite_sleep()
            
            pub_iso = meta.get("published_at") or ""
            pub_local_date = iso_to_local_date(pub_iso) or ""
            
            # Check if article is old enough to stop
            if pub_local_date and pub_local_date < end_date:
                pass  # Bài cũ, không ghi
            else:
                page_all_older_than_end = False
            
            # Only save articles >= end_date
            if (not pub_local_date) or (pub_local_date >= end_date):
                row = make_row(aurl, meta, category_fallback=category_slug)
                append_row(CSV_PATH, row)
                seen_urls.add(aurl)
                seen_ids.add(aid)
                added += 1
                page_added += 1
        
        print(f"[{category_slug}] Page {page}: Added {page_added} articles (total: {added})")
        
        # Stop if all articles on this page are older than end_date
        if page_all_older_than_end:
            print(f"[{category_slug}] All articles older than {end_date}, stopping")
            break
        
        page += 1
        polite_sleep()
    
    return added


def main():
    ensure_csv_header(CSV_PATH)
    seen_urls, seen_ids = load_seen_from_csv(CSV_PATH)
    
    print(f"Starting LaoDong HTML crawler...")
    print(f"END_DATE: {END_DATE}")
    print(f"CSV: {CSV_PATH}")
    print(f"Categories: {len(CATEGORY_URLS)}")
    
    total = 0
    for cat in CATEGORY_URLS:
        try:
            added = crawl_category(cat, END_DATE, seen_urls, seen_ids)
            print(f"✓ [{cat}] Added {added} articles\n")
            total += added
        except Exception as e:
            print(f"✗ [{cat}] ERROR: {e}\n")
    
    print(f"\n{'='*60}")
    print(f"Done! Total appended {total} rows to {CSV_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()


# # znews

# In[3]:


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import re
import time
import random
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VN_TZ = timezone(timedelta(hours=7))

# ================== CONFIG ==================
CATEGORY_URLS = [
    "https://znews.vn/xuat-ban.html",
    "https://znews.vn/kinh-doanh-tai-chinh.html",
    "https://znews.vn/suc-khoe.html",
    "https://znews.vn/the-thao.html",
    "https://znews.vn/doi-song.html",
    "https://znews.vn/cong-nghe.html",
    "https://znews.vn/giai-tri.html",
]

# Crawl từ mới -> cũ cho tới khi bài có ngày < END_DATE (theo giờ VN)
END_DATE = "2026-01-15"  # YYYY-MM-DD
MAX_PAGES_PER_CATEGORY = 2000  # safety stop

CSV_PATH = "znews_html_categories_vi.csv"

TIMEOUT = 25
REQUEST_DELAY_BASE = 0.25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ZNewsHTMLCrawler/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
# ===========================================

CSV_HEADER = [
    "id",
    "title",
    "published_at",
    "source.name",
    "url",
    "language",
    "category.primary",
    "keywords",
    "entities",
    "content.text",
]

SOURCE_NAME = "ZNews"
DEFAULT_LANGUAGE = "vi"
DEBUG = False

# ----- HTTP session with retry -----
session = requests.Session()
session.headers.update(HEADERS)

retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
    respect_retry_after_header=True,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)


def log(msg: str):
    if DEBUG:
        print(msg)


def polite_sleep():
    time.sleep(REQUEST_DELAY_BASE + random.uniform(0, 0.4))


def md5_id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def fetch_text(url: str) -> str:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def to_iso_utc(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if not dt:
            return None
        if dt.tzinfo is None:
            if VN_TZ:
                dt = dt.replace(tzinfo=VN_TZ)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def iso_to_local_date(iso_utc: str) -> Optional[str]:
    if not iso_utc:
        return None
    try:
        dt = dateparser.parse(iso_utc)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if VN_TZ:
            dt_local = dt.astimezone(VN_TZ)
        else:
            dt_local = dt
        return dt_local.date().isoformat()
    except Exception:
        return None


def ensure_csv_header(csv_path: str):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)


def load_seen_from_csv(csv_path: str) -> Tuple[Set[str], Set[str]]:
    seen_urls, seen_ids = set(), set()
    if not os.path.exists(csv_path):
        return seen_urls, seen_ids
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, None)
            if not header:
                return seen_urls, seen_ids
            id_idx = header.index("id") if "id" in header else 0
            url_idx = header.index("url") if "url" in header else 4
            for row in r:
                if len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u:
                        seen_urls.add(u)
                if len(row) > id_idx:
                    i = row[id_idx].strip()
                    if i:
                        seen_ids.add(i)
    except Exception:
        pass
    return seen_urls, seen_ids


def append_row(csv_path: str, row: Dict[str, Any]):
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([row.get(k, "") for k in CSV_HEADER])
        f.flush()


def extract_language_from_html(soup: BeautifulSoup) -> str:
    html_tag = soup.find("html")
    if html_tag:
        lang = html_tag.get("lang") or html_tag.get("xml:lang")
        if lang:
            lang = lang.lower().strip()
            if lang.startswith("vi"):
                return "vi"
            if lang.startswith("en"):
                return "en"
            return lang
    return DEFAULT_LANGUAGE


def extract_keywords_from_html(soup: BeautifulSoup) -> List[str]:
    for sel in ['meta[name="keywords"]', 'meta[name="news_keywords"]']:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            raw = tag["content"]
            kws = [x.strip() for x in raw.split(",") if x.strip()]
            seen = set()
            out = []
            for k in kws:
                if k not in seen:
                    seen.add(k)
                    out.append(k)
            return out
    return []


def extract_article_meta(article_html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(article_html, "lxml")

    # title
    title = ""
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        title = og["content"].strip()
    if not title:
        h1 = soup.select_one("h1.title-detail, h1.article-title, h1")
        if h1:
            title = h1.get_text(strip=True)

    # published_at
    pub = ""
    m_pub = soup.select_one('meta[property="article:published_time"]')
    if m_pub and m_pub.get("content"):
        pub = to_iso_utc(m_pub["content"].strip()) or ""
    if not pub:
        m2 = soup.select_one('meta[itemprop="datePublished"]')
        if m2 and m2.get("content"):
            pub = to_iso_utc(m2["content"].strip()) or ""
    if not pub:
        ttag = soup.select_one("time")
        if ttag:
            pub = to_iso_utc(ttag.get("datetime") or ttag.get_text(strip=True)) or ""

    # category.primary
    category_primary = ""
    sec = soup.select_one('meta[property="article:section"]')
    if sec and sec.get("content"):
        category_primary = sec["content"].strip()

    language = extract_language_from_html(soup)
    keywords = extract_keywords_from_html(soup)

    # content.text - ZNews thường dùng class .article-body, .the-article-body
    content_text = ""
    article_body = soup.select_one(".the-article-body")
    if not article_body:
        article_body = soup.select_one(".article-body")
    if not article_body:
        article_body = soup.select_one("article")
    if not article_body:
        article_body = soup.select_one(".content-detail")
    
    if article_body:
        paragraphs = article_body.find_all("p")
        text_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text:
                text_parts.append(text)
        content_text = " ".join(text_parts)

    return {
        "title": title,
        "published_at": pub,
        "language": language,
        "keywords": keywords,
        "category_from_article": category_primary,
        "entities": [],
        "content_text": content_text,
    }


def extract_article_urls_from_category_page(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")

    urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("/"):
            href = "https://znews.vn" + href
        if not href.startswith("https://znews.vn/"):
            continue
        # ZNews bài viết thường có format /ten-bai-post[ID].html
        if "-post" in href and ".html" in href:
            urls.append(href.split("?")[0])

    # unique giữ thứ tự
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def find_next_page_url(category_url: str, html: str, current_page: int) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    # thử rel=next
    ln = soup.select_one('link[rel="next"]')
    if ln and ln.get("href"):
        href = ln["href"].strip()
        if href.startswith("/"):
            href = "https://znews.vn" + href
        return href

    # thử tìm nút pagination
    a_next = soup.select_one('a.page-next, a[rel="next"]')
    if a_next and a_next.get("href"):
        href = a_next["href"].strip()
        if href.startswith("/"):
            href = "https://znews.vn" + href
        return href

    # fallback: ZNews dùng format /trangX.html
    # https://znews.vn/the-thao.html -> https://znews.vn/the-thao/trang2.html
    next_page = current_page + 1
    base_url = category_url.rstrip("/").replace(".html", "")
    return f"{base_url}/trang{next_page}.html"


def make_row(url: str, meta: Dict[str, Any], category_fallback: str) -> Dict[str, Any]:
    id_ = md5_id(url)
    category_primary = meta.get("category_from_article") or category_fallback
    keywords_str = "|".join(meta.get("keywords") or [])
    entities_str = "|".join(meta.get("entities") or [])

    return {
        "id": id_,
        "title": meta.get("title") or "",
        "published_at": meta.get("published_at") or "",
        "source.name": SOURCE_NAME,
        "url": url,
        "language": meta.get("language") or DEFAULT_LANGUAGE,
        "category.primary": category_primary or "",
        "keywords": keywords_str,
        "entities": entities_str,
        "content.text": meta.get("content_text") or "",
    }


def crawl_category(category_url: str, end_date: str, seen_urls: Set[str], seen_ids: Set[str]) -> Tuple[int, int, int]:
    """
    Crawl category và trả về (added, skipped_duplicate, skipped_old)
    - added: số bài mới được thêm vào CSV
    - skipped_duplicate: số bài bị trùng (đã có trong CSV)
    - skipped_old: số bài cũ hơn END_DATE
    """
    added = 0
    skipped_duplicate = 0
    skipped_old = 0
    page = 1
    url_page = category_url

    # extract category slug từ URL
    category_slug = category_url.rstrip("/").split("/")[-1].replace(".html", "")

    while page <= MAX_PAGES_PER_CATEGORY and url_page:
        html = fetch_text(url_page)
        article_urls = extract_article_urls_from_category_page(html)

        if DEBUG:
            log(f"[{category_slug}] page {page} got {len(article_urls)} candidate urls: {url_page}")

        if not article_urls:
            break

        page_all_older_than_end = True

        for aurl in article_urls:
            # Kiểm tra duplicate TRƯỚC KHI fetch HTML
            if aurl in seen_urls:
                skipped_duplicate += 1
                continue

            aid = md5_id(aurl)
            if aid in seen_ids:
                skipped_duplicate += 1
                continue

            try:
                ah = fetch_text(aurl)
                meta = extract_article_meta(ah)
            except Exception as e:
                log(f"[WARN] article fetch failed {aurl}: {e}")
                continue
            finally:
                polite_sleep()

            pub_iso = meta.get("published_at") or ""
            pub_local_date = iso_to_local_date(pub_iso) or ""

            # nếu có ngày và nhỏ hơn end_date => đánh dấu cũ
            if pub_local_date and pub_local_date < end_date:
                skipped_old += 1
                pass
            else:
                page_all_older_than_end = False

            # Nếu bài >= end_date thì ghi
            if (not pub_local_date) or (pub_local_date >= end_date):
                row = make_row(aurl, meta, category_fallback=category_slug)
                append_row(CSV_PATH, row)
                seen_urls.add(aurl)
                seen_ids.add(aid)
                added += 1

        # Nếu cả trang toàn bài cũ hơn end_date thì dừng category này
        if page_all_older_than_end:
            if DEBUG:
                log(f"[{category_slug}] stop: page {page} all older than end_date={end_date}")
            break

        # đi trang tiếp
        next_url = find_next_page_url(category_url, html, current_page=page)
        if not next_url or next_url == url_page:
            break
        if next_url == url_page:
            break
        url_page = next_url
        page += 1
        polite_sleep()

    return added, skipped_duplicate, skipped_old


def main():
    ensure_csv_header(CSV_PATH)
    seen_urls, seen_ids = load_seen_from_csv(CSV_PATH)

    print(f"=== ZNews Crawler - Cơ chế xử lý Duplicate ===")
    print(f"Đã load {len(seen_urls)} URLs và {len(seen_ids)} IDs từ CSV")
    print(f"END_DATE: {END_DATE}")
    print(f"Crawling {len(CATEGORY_URLS)} categories...\n")

    total_added = 0
    total_duplicates = 0
    total_old = 0
    
    for cat in CATEGORY_URLS:
        try:
            added, skipped_duplicate, skipped_old = crawl_category(cat, END_DATE, seen_urls, seen_ids)
            total_added += added
            total_duplicates += skipped_duplicate
            total_old += skipped_old
            
            # Tính tỷ lệ duplicate
            total_found = added + skipped_duplicate + skipped_old
            dup_rate = (skipped_duplicate / total_found * 100) if total_found > 0 else 0.0
            
            print(f"[{cat}]")
            print(f"  ✅ Added: {added} bài mới")
            print(f"  🔄 Duplicates: {skipped_duplicate} bài trùng")
            print(f"  ⏰ Old: {skipped_old} bài cũ (< {END_DATE})")
            print(f"  📊 Duplicate rate: {dup_rate:.1f}%\n")
            
        except Exception as e:
            print(f"[{cat}] ❌ ERROR: {e}\n")

    # Tổng kết
    grand_total = total_added + total_duplicates + total_old
    overall_dup_rate = (total_duplicates / grand_total * 100) if grand_total > 0 else 0.0
    
    print(f"\n{'='*60}")
    print(f"📈 TỔNG KẾT:")
    print(f"  ✅ Tổng bài mới thêm vào CSV: {total_added}")
    print(f"  🔄 Tổng bài trùng (bỏ qua): {total_duplicates}")
    print(f"  ⏰ Tổng bài cũ (bỏ qua): {total_old}")
    print(f"  📊 Tổng bài kiểm tra: {grand_total}")
    print(f"  💯 Tỷ lệ duplicate: {overall_dup_rate:.1f}%")
    print(f"{'='*60}")
    print(f"\n✅ Hoàn thành! Đã thêm {total_added} bài mới vào {CSV_PATH}")
    
    if overall_dup_rate > 70:
        print(f"💡 Gợi ý: Tỷ lệ duplicate cao ({overall_dup_rate:.1f}%) cho thấy crawler đang hoạt động tốt!")
    elif overall_dup_rate < 20 and total_duplicates > 0:
        print(f"⚠️  Lưu ý: Tỷ lệ duplicate thấp ({overall_dup_rate:.1f}%) - có thể có nhiều bài mới hoặc nguồn cập nhật thường xuyên")


if __name__ == "__main__":
    main()

# # 24h

# In[ ]:


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import re
import time
import random
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VN_TZ = timezone(timedelta(hours=7))

# ================== CONFIG ==================
RSS_FEEDS = [
    ("https://cdn.24h.com.vn/upload/rss/trangchu24h.rss", "trang-chu"),
    ("https://cdn.24h.com.vn/upload/rss/tintuctrongngay.rss", "tin-tuc-trong-ngay"),
    ("https://cdn.24h.com.vn/upload/rss/bongda.rss", "bong-da"),
    ("https://cdn.24h.com.vn/upload/rss/asiancup2019.rss", "the-thao"),
    ("https://cdn.24h.com.vn/upload/rss/thoitrang.rss", "thoi-trang"),
    ("https://cdn.24h.com.vn/upload/rss/thoitranghitech.rss", "hi-tech"),
    ("https://cdn.24h.com.vn/upload/rss/taichinhbatdongsan.rss", "tai-chinh-bat-dong-san"),
    ("https://cdn.24h.com.vn/upload/rss/phim.rss", "phim"),
    ("https://cdn.24h.com.vn/upload/rss/giaoducduhoc.rss", "giao-duc-du-hoc"),
    ("https://cdn.24h.com.vn/upload/rss/bantrecuocsong.rss", "ban-tre-cuoc-song"),
    ("https://cdn.24h.com.vn/upload/rss/thethao.rss", "the-thao"),
]

# Crawl từ mới -> cũ cho tới khi bài có ngày < END_DATE (theo giờ VN)
# Lưu ý: RSS của 24h chỉ cung cấp ~5 ngày data gần nhất
END_DATE = "2026-01-15"  # YYYY-MM-DD (điều chỉnh phù hợp với RSS limitation)

CSV_PATH = "24h_html_categories_vi.csv"

TIMEOUT = 25
REQUEST_DELAY_BASE = 0.25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; 24hHTMLCrawler/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
# ===========================================

CSV_HEADER = [
    "id",
    "title",
    "published_at",
    "source.name",
    "url",
    "language",
    "category.primary",
    "keywords",
    "entities",
    "content.text",
]

SOURCE_NAME = "24h"
DEFAULT_LANGUAGE = "vi"
DEBUG = False

# ----- HTTP session with retry -----
session = requests.Session()
session.headers.update(HEADERS)

retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
    respect_retry_after_header=True,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)


def log(msg: str):
    if DEBUG:
        print(msg)


def polite_sleep():
    time.sleep(REQUEST_DELAY_BASE + random.uniform(0, 0.4))


def md5_id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def fetch_text(url: str) -> str:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    # Xử lý encoding đặc biệt của 24h
    r.encoding = r.apparent_encoding or 'utf-8'
    return r.text


def fetch_rss(rss_url: str) -> feedparser.FeedParserDict:
    """Fetch và parse RSS feed, xử lý encoding đúng cách"""
    r = session.get(rss_url, timeout=TIMEOUT)
    r.raise_for_status()
    # Feedparser tự xử lý encoding
    feed = feedparser.parse(r.content)
    return feed


def to_iso_utc(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if not dt:
            return None
        if dt.tzinfo is None:
            if VN_TZ:
                dt = dt.replace(tzinfo=VN_TZ)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def iso_to_local_date(iso_utc: str) -> Optional[str]:
    if not iso_utc:
        return None
    try:
        dt = dateparser.parse(iso_utc)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if VN_TZ:
            dt_local = dt.astimezone(VN_TZ)
        else:
            dt_local = dt
        return dt_local.date().isoformat()
    except Exception:
        return None


def ensure_csv_header(csv_path: str):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)


def load_seen_from_csv(csv_path: str) -> Tuple[Set[str], Set[str]]:
    seen_urls, seen_ids = set(), set()
    if not os.path.exists(csv_path):
        return seen_urls, seen_ids
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, None)
            if not header:
                return seen_urls, seen_ids
            id_idx = header.index("id") if "id" in header else 0
            url_idx = header.index("url") if "url" in header else 4
            for row in r:
                if len(row) > url_idx:
                    u = row[url_idx].strip()
                    if u:
                        seen_urls.add(u)
                if len(row) > id_idx:
                    i = row[id_idx].strip()
                    if i:
                        seen_ids.add(i)
    except Exception:
        pass
    return seen_urls, seen_ids


def append_row(csv_path: str, row: Dict[str, Any]):
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([row.get(k, "") for k in CSV_HEADER])
        f.flush()


def extract_language_from_html(soup: BeautifulSoup) -> str:
    html_tag = soup.find("html")
    if html_tag:
        lang = html_tag.get("lang") or html_tag.get("xml:lang")
        if lang:
            lang = lang.lower().strip()
            if lang.startswith("vi"):
                return "vi"
            if lang.startswith("en"):
                return "en"
            return lang
    return DEFAULT_LANGUAGE


def extract_keywords_from_html(soup: BeautifulSoup) -> List[str]:
    for sel in ['meta[name="keywords"]', 'meta[name="news_keywords"]']:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            raw = tag["content"]
            kws = [x.strip() for x in raw.split(",") if x.strip()]
            seen = set()
            out = []
            for k in kws:
                if k not in seen:
                    seen.add(k)
                    out.append(k)
            return out
    return []


def extract_article_meta(article_html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(article_html, "lxml")

    # title
    title = ""
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        title = og["content"].strip()
    if not title:
        h1 = soup.select_one("h1.title-detail, h1.cate-24h-title-detail, h1")
        if h1:
            title = h1.get_text(strip=True)

    # published_at
    pub = ""
    m_pub = soup.select_one('meta[property="article:published_time"]')
    if m_pub and m_pub.get("content"):
        pub = to_iso_utc(m_pub["content"].strip()) or ""
    if not pub:
        m2 = soup.select_one('meta[itemprop="datePublished"]')
        if m2 and m2.get("content"):
            pub = to_iso_utc(m2["content"].strip()) or ""
    if not pub:
        ttag = soup.select_one("time")
        if ttag:
            pub = to_iso_utc(ttag.get("datetime") or ttag.get_text(strip=True)) or ""
    if not pub:
        # 24h có thể dùng class .cate-24h-date-published
        date_pub = soup.select_one(".cate-24h-date-published")
        if date_pub:
            pub = to_iso_utc(date_pub.get_text(strip=True)) or ""

    # category.primary
    category_primary = ""
    sec = soup.select_one('meta[property="article:section"]')
    if sec and sec.get("content"):
        category_primary = sec["content"].strip()

    language = extract_language_from_html(soup)
    keywords = extract_keywords_from_html(soup)

    # content.text - 24h thường dùng class .cate-24h-content-text
    content_text = ""
    article_body = soup.select_one(".cate-24h-content-text")
    if not article_body:
        article_body = soup.select_one("article .content-text")
    if not article_body:
        article_body = soup.select_one(".content-text")
    if not article_body:
        article_body = soup.select_one(".article-content")
    if not article_body:
        article_body = soup.select_one("article")
    
    if article_body:
        paragraphs = article_body.find_all("p")
        text_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text:
                text_parts.append(text)
        content_text = " ".join(text_parts)

    return {
        "title": title,
        "published_at": pub,
        "language": language,
        "keywords": keywords,
        "category_from_article": category_primary,
        "entities": [],
        "content_text": content_text,
    }


def make_row(url: str, meta: Dict[str, Any], category_fallback: str) -> Dict[str, Any]:
    id_ = md5_id(url)
    category_primary = meta.get("category_from_article") or category_fallback
    keywords_str = "|".join(meta.get("keywords") or [])
    entities_str = "|".join(meta.get("entities") or [])

    return {
        "id": id_,
        "title": meta.get("title") or "",
        "published_at": meta.get("published_at") or "",
        "source.name": SOURCE_NAME,
        "url": url,
        "language": meta.get("language") or DEFAULT_LANGUAGE,
        "category.primary": category_primary or "",
        "keywords": keywords_str,
        "entities": entities_str,
        "content.text": meta.get("content_text") or "",
    }


def crawl_rss_feed(rss_url: str, category_slug: str, end_date: str, 
                   seen_urls: Set[str], seen_ids: Set[str]) -> Tuple[int, int, int]:
    """
    Crawl articles từ RSS feed
    Returns: (added, skipped_duplicate, skipped_old)
    """
    added = 0
    skipped_old = 0
    skipped_duplicate = 0
    
    try:
        feed = fetch_rss(rss_url)
    except Exception as e:
        log(f"[WARN] RSS fetch failed {rss_url}: {e}")
        return (0, 0, 0)
    
    if not feed.entries:
        log(f"[WARN] No entries in RSS feed {rss_url}")
        return (0, 0, 0)
    
    for entry in feed.entries:
        article_url = entry.get("link", "").strip()
        if not article_url:
            continue
            
        # Normalize URL
        if not article_url.startswith("http"):
            article_url = "https://www.24h.com.vn" + article_url
        
        # Lấy published date từ RSS để check trước
        pub_date_rss = entry.get("published") or entry.get("updated")
        pub_iso_rss = to_iso_utc(pub_date_rss) if pub_date_rss else ""
        pub_local_date = iso_to_local_date(pub_iso_rss) or ""
        
        # Skip articles older than END_DATE trước khi check duplicate
        # Vì RSS được sắp xếp theo thời gian, có thể early exit
        if pub_local_date and pub_local_date < end_date:
            skipped_old += 1
            continue
            
        # Check duplicate - QUAN TRỌNG: Skip nếu đã crawl
        # Khi chạy hàng ngày, đa số articles sẽ bị skip ở đây
        if article_url in seen_urls:
            skipped_duplicate += 1
            continue
            
        aid = md5_id(article_url)
        if aid in seen_ids:
            skipped_duplicate += 1
            continue
        
        # Fetch full article content (chỉ với articles mới)
        try:
            article_html = fetch_text(article_url)
            meta = extract_article_meta(article_html)
        except Exception as e:
            log(f"[WARN] article fetch failed {article_url}: {e}")
            # Fallback: use RSS data
            meta = {
                "title": entry.get("title", ""),
                "published_at": pub_iso_rss,
                "language": DEFAULT_LANGUAGE,
                "keywords": [],
                "category_from_article": "",
                "entities": [],
                "content_text": BeautifulSoup(entry.get("summary", ""), "lxml").get_text(strip=True),
            }
        finally:
            polite_sleep()
        
        # Use RSS published date if article doesn't have one
        if not meta.get("published_at") and pub_iso_rss:
            meta["published_at"] = pub_iso_rss
        
        row = make_row(article_url, meta, category_fallback=category_slug)
        append_row(CSV_PATH, row)
        seen_urls.add(article_url)
        seen_ids.add(aid)
        added += 1
    
    # Always show summary for transparency
    print(f"  [{category_slug}] RSS entries: {len(feed.entries)} | Added: {added} | Duplicates: {skipped_duplicate} | Old: {skipped_old}")
    
    return (added, skipped_duplicate, skipped_old)


def main():
    print("="*80)
    print(f"24H.COM.VN CRAWLER - Duplicate-Safe Daily Crawling")
    print("="*80)
    
    ensure_csv_header(CSV_PATH)
    seen_urls, seen_ids = load_seen_from_csv(CSV_PATH)
    
    print(f"\n📊 Initial state:")
    print(f"  - Already crawled: {len(seen_urls)} URLs, {len(seen_ids)} IDs")
    print(f"  - Date filter: Articles >= {END_DATE}")
    print(f"  - Total feeds: {len(RSS_FEEDS)}")
    print()

    total_added = 0
    total_duplicates = 0
    total_old = 0
    
    for rss_url, category_slug in RSS_FEEDS:
        try:
            added, duplicates, old = crawl_rss_feed(rss_url, category_slug, END_DATE, seen_urls, seen_ids)
            total_added += added
            total_duplicates += duplicates
            total_old += old
        except Exception as e:
            print(f"  [{category_slug}] ERROR: {e}")

    print()
    print("="*80)
    print(f"✅ CRAWL SUMMARY")
    print("="*80)
    print(f"📝 New articles added: {total_added}")
    print(f"🔁 Duplicates skipped: {total_duplicates} (already in CSV)")
    print(f"⏰ Old articles skipped: {total_old} (before {END_DATE})")
    print(f"📊 Total processed: {total_added + total_duplicates + total_old}")
    print(f"💾 Output: {CSV_PATH}")
    print(f"📈 Total in CSV now: {len(seen_urls) + total_added} articles")
    print("="*80)
    
    if total_duplicates > 0:
        efficiency = (total_duplicates / (total_added + total_duplicates + total_old) * 100) if (total_added + total_duplicates + total_old) > 0 else 0
        print(f"\n💡 Duplicate rate: {efficiency:.1f}% - Perfect for daily runs!")
        print(f"   (High rate = most articles already crawled = efficient)")
    print()


if __name__ == "__main__":
    main()

# In[6]:


import pandas as pd
df_tonghop =pd.read_csv("rss_feed_articles_v2.csv")
df_laodong = pd.read_csv("laodong_html_articles_vi.csv")
df_znews = pd.read_csv("znews_html_categories_vi.csv")
df_24h = pd.read_csv("24h_html_categories_vi.csv")
print(f"  ✅ Tổng bài tonghop: {len(df_tonghop)}")
print(f"  ✅ Tổng bài laodong: {len(df_laodong)}")
print(f"  ✅ Tổng bài znews: {len(df_znews)}")
print(f"  ✅ Tổng bài 24h: {len(df_24h)}")
