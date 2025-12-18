import advertools as adv
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
TARGET_SITEMAP = "https://www.nytimes.com/sitemaps/new/news.xml.gz"
MASTER_FILE = "data/nyt_master_data.csv"
DAILY_FOLDER = "data/daily"
# ---------------------

print(f"[{datetime.now()}] Starting Job...")

# Ensure folders exist
os.makedirs(DAILY_FOLDER, exist_ok=True)

# 1. CRAWL
print("Fetching current sitemap...")
try:
    current_df = adv.sitemap_to_df(TARGET_SITEMAP)
    
    if 'lastmod' in current_df.columns:
        # Prepare new data
        new_data = current_df[['loc', 'lastmod']].copy()
        new_data['lastmod'] = pd.to_datetime(new_data['lastmod'])
        new_data.columns = ['url', 'date']
        
        # 2. UPDATE MASTER DATABASE
        if os.path.exists(MASTER_FILE):
            print("Loading existing database...")
            master_df = pd.read_csv(MASTER_FILE)
            master_df['date'] = pd.to_datetime(master_df['date'])
            combined_df = pd.concat([master_df, new_data])
        else:
            print("Creating new database...")
            combined_df = new_data

        # Deduplicate the Master (Keep latest)
        combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
        combined_df = combined_df.sort_values(by='date', ascending=False)
        
        # Save Master
        combined_df.to_csv(MASTER_FILE, index=False)
        print(f"Master Database updated: {len(combined_df)} total articles.")

        # 3. CREATE DAILY PARTITIONS
        # This loops through every unique date found in your data
        # and saves a specific file for that day.
        
        # We only look at dates present in the 'new_data' to save processing time,
        # plus the last few days from master to ensure we catch updates.
        dates_to_update = combined_df['date'].dt.date.unique()
        
        print(f"Organizing data into daily files...")
        
        for date_obj in dates_to_update:
            date_str = str(date_obj) # e.g., "2025-12-18"
            
            # Filter the master list for ONLY this day
            daily_slice = combined_df[combined_df['date'].dt.date == date_obj]
            
            # Save to data/daily/YYYY-MM-DD.csv
            daily_filename = f"{DAILY_FOLDER}/{date_str}.csv"
            daily_slice.to_csv(daily_filename, index=False)
            
        print("Daily files organized successfully.")
        
    else:
        print("Error: No 'lastmod' found in sitemap.")

except Exception as e:
    print(f"Critical Error: {e}")
    exit(1)
