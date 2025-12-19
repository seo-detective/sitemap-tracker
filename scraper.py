import advertools as adv
import pandas as pd
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
SITEMAPS = {
    "nytimes": "https://www.nytimes.com/sitemaps/new/news.xml.gz",
    "wsj": "https://www.wsj.com/sitemaps/news.xml", # Assuming you have WSJ
    "forbes": "https://www.forbes.com/sitemaps/sitemap.xml",
    "bloomberg": "https://www.bloomberg.com/feeds/sitemap_news.xml"
}

HISTORY_FILE = "data/recent_history.csv" 
DAILY_FOLDER = "data/daily"
# ---------------------

print(f"[{datetime.now()}] Starting Job...")

# Ensure folders exist
os.makedirs(DAILY_FOLDER, exist_ok=True)

all_new_data = []

# 1. LOOP THROUGH SITEMAPS
for site_name, sitemap_url in SITEMAPS.items():
    print(f"Fetching {site_name}...")
    try:
        df = adv.sitemap_to_df(sitemap_url, recursive=True)
        
        if 'lastmod' in df.columns:
            site_data = df[['loc', 'lastmod']].copy()
            
            # FIX #1: Handle incoming dates flexibly
            site_data['lastmod'] = pd.to_datetime(site_data['lastmod'], format='mixed')
            
            site_data.columns = ['url', 'date']
            site_data['publication'] = site_name
            
            all_new_data.append(site_data)
            print(f"-> Found {len(site_data)} URLs for {site_name}")
        else:
            print(f"-> Error: {site_name} has no dates.")
            
    except Exception as e:
        print(f"-> Failed to crawl {site_name}: {e}")

# 2. PROCESS DATA
if all_new_data:
    new_combined_df = pd.concat(all_new_data)
    
    # Load History (if exists)
    if os.path.exists(HISTORY_FILE):
        print("Loading existing database...")
        history_df = pd.read_csv(HISTORY_FILE)
        
        # FIX #2: Handle existing history dates flexibly (The crash happened here)
        history_df['date'] = pd.to_datetime(history_df['date'], format='mixed')
        
        final_df = pd.concat([history_df, new_combined_df])
    else:
        final_df = new_combined_df

    # Deduplicate (Keep latest version)
    final_df = final_df.drop_duplicates(subset=['url'], keep='last')
    final_df = final_df.sort_values(by=['date', 'publication'], ascending=[False, True])

    # 3. SAVE DAILY FILES
    dates_to_update = new_combined_df['date'].dt.date.unique()
    
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
    cutoff_date = datetime.now() - timedelta(days=30)
    
    # Ensure cutoff_date is timezone-aware if your data is
    if final_df['date'].dt.tz is not None:
         cutoff_date = pd.Timestamp.now(tz='UTC') - timedelta(days=30)

    recent_history = final_df[final_df['date'] > cutoff_date]
    
    recent_history.to_csv(HISTORY_FILE, index=False)
    print(f"History file pruned. Keeping {len(recent_history)} rows.")

else:
    print("No data found.")
