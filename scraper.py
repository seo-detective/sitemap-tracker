import advertools as adv
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
TARGET_SITEMAP = "https://www.nytimes.com/sitemaps/new/news.xml.gz"
DATABASE_FILE = "data/nyt_master_data.csv"
# ---------------------

print(f"[{datetime.now()}] Starting Job...")

# Ensure the 'data' folder exists
if not os.path.exists('data'):
    os.makedirs('data')

# 1. CRAWL
print("Fetching current sitemap...")
try:
    current_df = adv.sitemap_to_df(TARGET_SITEMAP)
    
    if 'lastmod' in current_df.columns:
        # Prepare new data
        new_data = current_df[['loc', 'lastmod']].copy()
        new_data['lastmod'] = pd.to_datetime(new_data['lastmod'])
        new_data.columns = ['url', 'date']
        
        # 2. LOAD & MERGE
        if os.path.exists(DATABASE_FILE):
            print("Loading existing database...")
            master_df = pd.read_csv(DATABASE_FILE)
            master_df['date'] = pd.to_datetime(master_df['date'])
            combined_df = pd.concat([master_df, new_data])
        else:
            print("Creating new database...")
            combined_df = new_data

        # 3. DEDUPLICATE (Keep the latest version of duplicates)
        combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
        
        # Sort by date (newest first)
        combined_df = combined_df.sort_values(by='date', ascending=False)

        # 4. SAVE
        combined_df.to_csv(DATABASE_FILE, index=False)
        print(f"Success! Total articles tracked: {len(combined_df)}")
        
    else:
        print("Error: No 'lastmod' found in sitemap.")

except Exception as e:
    print(f"Critical Error: {e}")
    exit(1) # Fail the action so you get an email alert
