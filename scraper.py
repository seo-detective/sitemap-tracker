import pandas as pd
import os
import requests
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
SITEMAPS = {
    "nytimes": "https://www.nytimes.com/sitemaps/new/news.xml.gz",
    "wsj": "https://www.wsj.com/wsjsitemaps/wsj_google_news.xml",
    "forbes": "https://www.forbes.com/news_sitemap.xml",  # UPDATED URL
    "bloomberg": "https://www.bloomberg.com/sitemaps/news/latest.xml",
    "insider": "https://www.businessinsider.com/sitemap/google-news.xml",
}

HISTORY_FILE = "data/recent_history.csv" 
DAILY_FOLDER = "data/daily"

# --- HEADERS ---
HEADERS_CHROME = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
    "Accept-Encoding": "gzip, deflate"
}

HEADERS_GOOGLEBOT = {
    "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Accept": "application/xml, text/xml, */*",
    "Accept-Encoding": "gzip, deflate"
}
# ---------------------

print(f"[{datetime.now()}] Starting Job...")
os.makedirs(DAILY_FOLDER, exist_ok=True)

all_new_data = [] 

# --- SETUP RETRY SESSION ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def parse_sitemap(content, site_name):
    urls = []
    try:
        root = ET.fromstring(content)
        for child in root:
            if 'url' in child.tag.lower():
                url_data = {'url': None, 'date': None, 'publication': site_name}
                
                # Helper function to check a tag for data
                def check_tag(tag):
                    tag_name = tag.tag.lower()
                    if 'loc' in tag_name:
                        url_data['url'] = tag.text.strip()
                    elif 'lastmod' in tag_name or 'publication_date' in tag_name:
                        url_data['date'] = tag.text.strip()

                # 1. Check direct children
                for sub in child:
                    check_tag(sub)
                    
                    # 2. Check nested children (Fix for Insider)
                    if 'news' in sub.tag.lower():
                        for subsub in sub:
                            check_tag(subsub)
                            
                if url_data['url']:
                    urls.append(url_data)
                    
    except Exception as e:
        print(f"-> XML Parse Error for {site_name}: {e}")
    return urls

# 1. LOOP THROUGH SITEMAPS
for site_name, sitemap_url in SITEMAPS.items():
    print(f"Fetching {site_name}...")
    
    # Use Googlebot for WSJ, Chrome for others
    if site_name == 'wsj':
        session.headers.update(HEADERS_GOOGLEBOT)
    else:
        session.headers.update(HEADERS_CHROME)
    
    try:
        # Standard timeout
        response = session.get(sitemap_url, timeout=30)
        
        if response.status_code != 200:
            print(f"-> Failed to download {site_name}: HTTP {response.status_code}")
            continue

        content = response.content
        if content.startswith(b'\x1f\x8b'):
            try:
                content = gzip.decompress(content)
            except OSError:
                pass 

        extracted_data = parse_sitemap(content, site_name)
        
        if extracted_data:
            df = pd.DataFrame(extracted_data)
            
            # Normalize Date
            df['date'] = pd.to_datetime(df['date'], format='mixed', utc=True, errors='coerce')
            
            # Clean Data
            df = df.dropna(subset=['url'])
            df = df.dropna(subset=['date'])

            all_new_data.append(df)
            print(f"-> Found {len(df)} articles for {site_name}")
        else:
            print(f"-> Warning: No articles found in {site_name}")

    except Exception as e:
        print(f"-> Critical Error processing {site_name}: {e}")

# 2. PROCESS DATA
if all_new_data:
    new_combined_df = pd.concat(all_new_data)
    
    if os.path.exists(HISTORY_FILE):
        print("Loading existing database...")
        history_df = pd.read_csv(HISTORY_FILE)
        history_df['date'] = pd.to_datetime(history_df['date'], format='mixed', utc=True, errors='coerce')
        final_df = pd.concat([history_df, new_combined_df])
    else:
        final_df = new_combined_df

    final_df = final_df.drop_duplicates(subset=['url'], keep='last')
    final_df = final_df.dropna(subset=['date']) 
    final_df = final_df.sort_values(by=['date', 'publication'], ascending=[False, True])

    dates_to_update = final_df['date'].dt.date.unique()
    
    print(f"Updating daily files...")
    for date_obj in dates_to_update:
        date_str = str(date_obj)
        daily_filename = f"{DAILY_FOLDER}/{date_str}.csv"
        daily_slice = final_df[final_df['date'].dt.date == date_obj]
        daily_slice.to_csv(daily_filename, index=False)
        print(f"-> Saved {daily_filename} ({len(daily_slice)} articles)")

    cutoff_date = pd.Timestamp.now(tz='UTC') - timedelta(days=30)
    recent_history = final_df[final_df['date'] > cutoff_date]
    recent_history.to_csv(HISTORY_FILE, index=False)
    print(f"History file pruned. Keeping {len(recent_history)} rows.")
else:
    print("No data found.")
