import pandas as pd
import time
import os
from datetime import datetime

SOURCE_CSV = "/home/jovyan/work/stock prices.csv"                   
LANDING_ZONE = "/home/jovyan/work/data/stock_batches"        #where daily batch files will be stored
os.makedirs(LANDING_ZONE, exist_ok=True)

def run_simulator():
    try:
        df = pd.read_csv(SOURCE_CSV)
        df.columns = df.columns.str.strip()   # clean column names
        print(f" Loaded {len(df)} rows from {SOURCE_CSV}")
    except Exception as e:
        print(f" Error reading CSV: {e}")
        return

    required_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f" Missing columns: {missing}")
        return

    df['date'] = pd.to_datetime(df['date'])

    unique_dates = sorted(df['date'].unique())
    print(f"Simulating {len(unique_dates)} daily batches...")

    for i, d in enumerate(unique_dates, start=1):
        batch = df[df['date'] == d].copy()
        batch['processing_ts'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        date_str = d.strftime('%Y-%m-%d')
        file_name = f"stock_batch_{date_str}.csv"
        file_path = os.path.join(LANDING_ZONE, file_name)
        
        batch.to_csv(file_path, index=False)
        
        print(f" Batch {i}/{len(unique_dates)}: {date_str} → {len(batch)} records | File: {file_name}")
        
        time.sleep(1)

    print(f"\nDone! All batches saved in '{LANDING_ZONE}'")

if __name__ == "__main__":
    try:
        run_simulator()
    except KeyboardInterrupt:
        print("Simulator stopped by user.")
