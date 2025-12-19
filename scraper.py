import pandas as pd
import os
import requests
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

# --- CONFIGURATION ---
SITEMAPS = {
    "nytimes": "https://www.nytimes.com/sitemaps/new/news.xml.gz",
    "wsj": "https://www.wsj.com/wsjsitemaps/wsj_google_news.xml",
    "forbes": "https://www.forbes.com/news_sitemap.xml",  # UPDATED URL
    "bloomberg": "https://www.bloomberg.com/sitemaps/news/latest.xml"
}

HISTORY_FILE = "data/recent_history.csv" 
DAILY_FOLDER = "data/daily"

# --- HEADERS ---
# 1. Standard Chrome (For most sites)
HEADERS_CHROME = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
    # REMOVED 'br' TO PREVENT BROTLI COMPRESSION ISSUES
    "Accept-Encoding": "gzip, deflate"
}

# 2. Googlebot (Strictly for WSJ)
HEADERS_GOOGLEBOT = {
    "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Accept": "application/xml, text/xml, */*",
    # REMOVED 'br' HERE TOO
    "Accept-Encoding": "gzip, deflate"
}
# ---------------------

print(f"[{datetime.now()}] Starting Job...")
os.makedirs(DAILY_FOLDER, exist_ok=True)

all_new_data = []

def parse_sitemap(content, site_name):
    """
    Parses XML content (bytes) and extracts URLs + Dates.
    """
    urls = []
    try:
        # Parse XML
        root = ET.fromstring(content)
        
        # Iterate over all children to find <url> tags
        for child in root:
            if 'url' in child.tag.lower():
                url_data = {'url': None, 'date': None, 'publication': site_name}
                
                for sub in child:
                    tag_name = sub.tag.lower()
                    
                    # 1. Extract URL (loc)
                    if 'loc' in tag_name:
                        url_data['url'] = sub.text.strip()
                    
                    # 2. Extract Date (lastmod OR news:publication_date)
                    if 'lastmod' in tag_name:
                        url_data['date'] = sub.text.strip()
                    elif 'publication_date' in tag_name:
                        url_data['date'] = sub.text.strip()
                        
                if url_data['url']:
                    urls.append(url_data)
                    
    except ET.ParseError as e:
        print(f"-> XML Parse Error for {site_name}: {e}")
    except Exception as e:
        print(f"-> Error: {e}")
        
    return urls


# 1. LOOP THROUGH SITEMAPS
for site_name, sitemap_url in SITEMAPS.items():
    print(f"Fetching {site_name}...")
    
    # CHOOSE IDENTITY
    current_headers = HEADERS_GOOGLEBOT if site_name == 'wsj' else HEADERS_CHROME
    
    try:
        response = requests.get(sitemap_url, headers=current_headers, timeout=20)
        
        if response.status_code != 200:
            print(f"-> Failed to download {site_name}: HTTP {response.status_code}")
            continue

        # Intelligent GZIP handling
        content = response.content
        
        # Check for gzip magic number (1f 8b)
        if content.startswith(b'\x1f\x8b'):
            try:
                content = gzip.decompress(content)
            except OSError:
                pass 

        # Parse the content
        extracted_data = parse_sitemap(content, site_name)
        
        if extracted_data:
            df = pd.DataFrame(extracted_data)
            
            # Normalize Date
            df['date'] = pd.to_datetime(df['date'], format='mixed', utc=True, errors='coerce')
            
            # Remove rows with no URL
            df = df.dropna(subset=['url'])
            
            all_new_data.append(df)
            print(f"-> Found {len(df)} articles for {site_name}")
        else:
            print(f"-> Warning: No articles found in {site_name} (Parser returned 0)")

    except Exception as e:
        print(f"-> Critical Error processing {site_name}: {e}")

# 2. PROCESS DATA
if all_new_data:
    new_combined_df = pd.concat(all_new_data)
    
    # Load History
    if os.path.exists(HISTORY_FILE):
        print("Loading existing database...")
        history_df = pd.read_csv(HISTORY_FILE)
        history_df['date'] = pd.to_datetime(history_df['date'], format='mixed', utc=True, errors='coerce')
        final_df = pd.concat([history_df, new_combined_df])
    else:
        final_df = new_combined_df

    # Deduplicate
    final_df = final_df.drop_duplicates(subset=['url'], keep='last')
    final_df = final_df.dropna(subset=['date']) 
    final_df = final_df.sort_values(by=['date', 'publication'], ascending=[False, True])

    # 3. SAVE DAILY FILES
    dates_to_update = final_df['date'].dt.date.unique()
    
    print(f"Updating daily files...")
    for date_obj in dates_to_update:
        date_str = str(date_obj)
        daily_filename = f"{DAILY_FOLDER}/{date_str}.csv"
        
        daily_slice = final_df[final_df['date'].dt.date == date_obj]
        daily_slice.to_csv(daily_filename, index=False)
        print(f"-> Saved {daily_filename} ({len(daily_slice)} articles)")

    # 4. PRUNE HISTORY
    cutoff_date = pd.Timestamp.now(tz='UTC') - timedelta(days=30)
    recent_history = final_df[final_df['date'] > cutoff_date]
    recent_history.to_csv(HISTORY_FILE, index=False)
    print(f"History file pruned. Keeping {len(recent_history)} rows.")

else:
    print("No data found.")

# --- END OF SCRIPT ---
