import advertools as adv
import pandas as pd
import os
import requests  # NEW IMPORT
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

# TRICK: Identify as Googlebot. 
# Many news sites allow this even if they block generic scripts.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Accept": "application/xml, text/xml, */*",
    "Accept-Encoding": "gzip, deflate, br"
}
# ---------------------

print(f"[{datetime.now()}] Starting Job...")
os.makedirs(DAILY_FOLDER, exist_ok=True)

all_new_data = []

def parse_sitemap(content, site_name):
    """
    Parses XML content (bytes) and extracts URLs + Dates.
    Handles both standard sitemaps and News sitemaps.
    """
    urls = []
    
    try:
        # Parse XML
        root = ET.fromstring(content)
        
        # XML Namespaces are annoying. This usually works to find all 'url' tags
        # regardless of the namespace (e.g. <url>, <n:url>, etc.)
        # We iterate over all children and look for local names.
        
        for child in root:
            # We are looking for <url> blocks
            if 'url' in child.tag.lower():
                url_data = {'url': None, 'date': None, 'publication': site_name}
                
                for sub in child:
                    tag_name = sub.tag.lower()
                    
                    # 1. Extract URL (loc)
                    if 'loc' in tag_name:
                        url_data['url'] = sub.text.strip()
                    
                    # 2. Extract Date (lastmod OR news:publication_date)
                    # We check for any tag that contains 'date' or 'lastmod'
                    if 'lastmod' in tag_name:
                        url_data['date'] = sub.text.strip()
                    elif 'publication_date' in tag_name:
                        url_data['date'] = sub.text.strip()
                        
                if url_data['url']:
                    urls.append(url_data)
                    
    except Exception as e:
        print(f"-> XML Parsing Error for {site_name}: {e}")
        
    return urls


# 1. LOOP THROUGH SITEMAPS
for site_name, sitemap_url in SITEMAPS.items():
    print(f"Fetching {site_name}...")
    try:
        # Download with custom headers
        response = requests.get(sitemap_url, headers=HEADERS, timeout=20)
        
        if response.status_code != 200:
            print(f"-> Failed to download {site_name}: HTTP {response.status_code}")
            continue

        # Handle GZIP (NYTimes uses .gz)
        if sitemap_url.endswith(".gz"):
            try:
                content = gzip.decompress(response.content)
            except OSError:
                print(f"-> Error: {site_name} returned non-gzip data despite .gz extension.")
                content = response.content
        else:
            content = response.content

        # Parse the content
        extracted_data = parse_sitemap(content, site_name)
        
        if extracted_data:
            df = pd.DataFrame(extracted_data)
            
            # Normalize Date
            # 'coerce' turns bad dates into NaT so script doesn't crash
            df['date'] = pd.to_datetime(df['date'], format='mixed', utc=True, errors='coerce')
            
            # Fallback: if date is missing, use "now" (or drop them if you prefer)
            # df['date'] = df['date'].fillna(pd.Timestamp.now(tz='UTC'))
            
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
        final_
