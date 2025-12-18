import advertools as adv
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
# Format: "Name": "Sitemap URL"
SITEMAPS = {
    "nytimes": "https://www.nytimes.com/sitemaps/new/news.xml.gz",
    "wsj": "https://www.wsj.com/wsjsitemaps/wsj_google_news.xml",
    "wapo": "https://www.washingtonpost.com/sitemaps/news-sitemap.xml.gz",
    "forbes": "https://www.forbes.com/news_sitemap.xml",
    "bloomberg": "https://www.bloomberg.com/sitemaps/news/latest.xml",
}

MASTER_FILE = "data/master_data.csv"
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
        # Crawl
        df = adv.sitemap_to_df(sitemap_url, recursive=True)
        
        if 'lastmod' in df.columns:
            # Clean and Select
            site_data = df[['loc', 'lastmod']].copy()
            site_data['lastmod'] = pd.to_datetime(site_data['lastmod'])
            site_data.columns = ['url', 'date']
            
            # ADD THE NEW COLUMN: PUBLICATION NAME
            site_data['publication'] = site_name
            
            # Add to our list
            all_new_data.append(site_data)
            print(f"-> Found {len(site_data)} URLs for {site_name}")
        else:
            print(f"-> Error: {site_name} sitemap has no dates.")
            
    except Exception as e:
        print(f"-> Failed to crawl {site_name}: {e}")

# 2. COMBINE ALL NEW DATA
if all_new_data:
    new_combined_df = pd.concat(all_new_data)
    
    # 3. UPDATE MASTER DATABASE
    if os.path.exists(MASTER_FILE):
        print("Loading existing database...")
        master_df = pd.read_csv(MASTER_FILE)
        master_df['date'] = pd.to_datetime(master_df['date'])
        
        # Merge old and new
        final_df = pd.concat([master_df, new_combined_df])
    else:
        print("Creating new database...")
        final_df = new_combined_df

    # Deduplicate (Keep latest)
    final_df = final_df.drop_duplicates(subset=['url'], keep='last')
    
    # Sort: First by Date (newest), then by Publication
    final_df = final_df.sort_values(by=['date', 'publication'], ascending=[False, True])
    
    # Save Master
    final_df.to_csv(MASTER_FILE, index=False)
    print(f"Master Database updated: {len(final_df)} total articles.")

    # 4. CREATE DAILY PARTITIONS
    # We loop through dates in the NEW data to update daily files
    dates_to_update = new_combined_df['date'].dt.date.unique()
    
    print(f"Organizing data into daily files...")
    for date_obj in dates_to_update:
        date_str = str(date_obj)
        
        # Filter master for this day
        daily_slice = final_df[final_df['date'].dt.date == date_obj]
        
        # Save
        daily_filename = f"{DAILY_FOLDER}/{date_str}.csv"
        daily_slice.to_csv(daily_filename, index=False)
        
    print("Daily files organized successfully.")

else:
    print("No data found from any sitemaps.")
