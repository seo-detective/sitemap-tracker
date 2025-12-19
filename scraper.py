import advertools as adv
import pandas as pd
import os
import requests  # NEW IMPORT
from datetime import datetime, timedelta

# --- CONFIGURATION ---
SITEMAPS = {
    "nytimes": "https://www.nytimes.com/sitemaps/new/news.xml.gz",
    "wsj": "https://www.wsj.com/sitemaps/news.xml",
    "forbes": "https://www.forbes.com/news_sitemap.xml",  # UPDATED URL
    "bloomberg": "https://www.bloomberg.com/feeds/sitemap_news.xml"
}

HISTORY_FILE = "data/recent_history.csv" 
DAILY_FOLDER = "data/daily"

# NEW: Fake headers to look like a real Chrome browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
# ---------------------

print(f"[{datetime.now()}] Starting Job...")

# Ensure folders exist
os.makedirs(DAILY_FOLDER, exist_ok=True)

all_new_data = []

# 1. LOOP THROUGH SITEMAPS
for site_name, sitemap_url in SITEMAPS.items():
    print(f"Fetching {site_name}...")
    try:
        # STEP 1: Download manually with Headers (Fixes WSJ 403)
        response = requests.get(sitemap_url, headers=HEADERS, timeout=20)
        
        if response.status_code != 200:
            print(f"-> Failed to download {site_name}: HTTP {response.status_code}")
            continue

        # Save to a temp file so advertools can read it
        # We use .gz extension if the URL has it, otherwise .xml
        temp_ext = ".xml.gz" if sitemap_url.endswith(".gz") else ".xml"
        temp_filename = f"temp_sitemap{temp_ext}"
        
        with open(temp_filename, "wb") as f:
            f.write(response.content)

        # STEP 2: Parse the local file with advertools
        # recursive=False is safer here since we are downloading manually and these are usually flat news files
        df = adv.sitemap_to_df(temp_filename, recursive=False)
        
        # STEP 3: Normalize Date Columns (Fixes Bloomberg)
        # Bloomberg often uses 'news_publication_date' instead of 'lastmod'
        if 'news_publication_date' in df.columns:
            # Create/Fill lastmod using the news date if lastmod is missing
            if 'lastmod' not in df.columns:
                df['lastmod'] = df['news_publication_date']
            else:
                df['lastmod'] = df['lastmod'].fillna(df['news_publication_date'])

        if 'lastmod' in df.columns:
            site_data = df[['loc', 'lastmod']].copy()
            
            # Convert to datetime
            site_data['lastmod'] = pd.to_datetime(site_data['lastmod'], format='mixed', utc=True)
            
            site_data.columns = ['url', 'date']
            site_data['publication'] = site_name
            
            all_new_data.append(site_data)
            print(f"-> Found {len(site_data)} URLs for {site_name}")
        else:
            print(f"-> Error: {site_name} has no dates (checked lastmod and news_publication_date).")
        
        # Cleanup temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            
    except Exception as e:
        print(f"-> Failed to crawl {site_name}: {e}")

# 2. PROCESS DATA
if all_new_data:
    new_combined_df = pd.concat(all_new_data)
    
    # Load History (if exists)
    if os.path.exists(HISTORY_FILE):
        print("Loading existing database...")
        history_df = pd.read_csv(HISTORY_FILE)
        
        history_df['date'] = pd.to_datetime(history_df['date'], format='mixed', utc=True)
        
        final_df = pd.concat([history_df, new_combined_df])
    else:
        final_df = new_combined_df

    # Deduplicate (Keep latest version)
    final_df = final_df.drop_duplicates(subset=['url'], keep='last')
    final_df = final_df.sort_values(by=['date', 'publication'], ascending=[False, True])

    # 3. SAVE DAILY FILES
    # Convert to date object for grouping
    dates_to_update = final_df['date'].dt.date.unique()
    
    print(f"Updating daily files...")
    for date_obj in dates_to_update:
        date_str = str(date_obj)
        daily_filename = f"{DAILY_FOLDER}/{date_str}.csv"
        
        # Get data for this specific day
        daily_slice = final_df[final_df['date'].dt.date == date_obj]
        
        # Save it
        daily_slice.to_csv(daily_filename, index=False)
        print(f"-> Saved {daily_filename} ({len(daily_slice)} articles)")

    # 4. PRUNE HISTORY
    # Use timezone-aware UTC time for comparison
    cutoff_date = pd.Timestamp.now(tz='UTC') - timedelta(days=30)

    recent_history = final_df[final_df['date'] > cutoff_date]
    
    recent_history.to_csv(HISTORY_FILE, index=False)
    print(f"History file pruned. Keeping {len(recent_history)} rows.")

else:
    print("No data found.")
