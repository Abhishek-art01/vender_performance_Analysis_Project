import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import logging
from tqdm.notebook import tqdm
import io
import sys
from pathlib import Path

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Connection ---
from credintial import db_config
conn_str = (
    f"postgresql+psycopg2://{db_config.DB1_CONFIG['user']}:"
    f"{db_config.DB1_CONFIG['password']}@"
    f"{db_config.DB1_CONFIG['host']}:"
    f"{db_config.DB1_CONFIG['port']}/"
    f"{db_config.DB1_CONFIG['dbname']}"
)
engine = create_engine(conn_str)

# --- Connection Test ---
with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print("✅ Connected successfully!")
    print(result.fetchone())

def load_data_fast(data_path, engine, chunksize=100000):
    start = time.time()
    
    print(f"\n🔍 Checking for files in: {data_path}")
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    print(f"Found {len(csv_files)} CSV files: {csv_files}")

    if not csv_files:
        print("❌ No CSV files found in that directory. Script will now exit.")
        return

    for file in csv_files:
        print(f"\n--- 🚀 Starting file: {file} ---")
        
        table_name = os.path.splitext(file)[0].lower()
        file_path = os.path.join(data_path, file)
        
        # --- 1. ADDED THIS BACK ---
        # This will count all rows *before* uploading, which causes a delay.
        print(f"Pre-counting rows in {file} for progress bar... (This may take a moment)")
        total_rows = sum(1 for row in open(file_path, 'r', encoding='utf-8')) - 1 # subtract header
        print(f"Found {total_rows} total rows.")
        
        reader = pd.read_csv(file_path, chunksize=chunksize, iterator=True)
        
        # --- 2. MODIFIED THIS ---
        # Added total=total_rows to the tqdm call to get the percentage bar
        with tqdm(total=total_rows, desc=f"Uploading {file}", unit="rows") as pbar:
            raw_conn = None
            try:
                # 1. --- FIRST CHUNK ---
                first_chunk = next(reader)
                
                print(f"Inserting first chunk for {file} to create table...")
                first_chunk.to_sql(table_name, engine, index=False, 
                                   if_exists='append', method='multi')
                pbar.update(len(first_chunk))
                print("First chunk inserted.")

                # 2. --- ALL OTHER CHUNKS ---
                print("Switching to fast COPY method for remaining chunks...")
                
                raw_conn = engine.raw_connection()
                
                with raw_conn.cursor() as cursor:
                    for chunk in reader:
                        buffer = io.StringIO()
                        chunk.to_csv(buffer, index=False, header=False)
                        buffer.seek(0)
                        
                        cols = ','.join(f'"{c}"' for c in chunk.columns)
                        sql = f"COPY {table_name} ({cols}) FROM STDIN WITH CSV"
                        cursor.copy_expert(sql, buffer)
                        
                        pbar.update(len(chunk))
                
                raw_conn.commit()
                logging.info(f"Ingested {file} into database.")

            except StopIteration:
                logging.warning(f"File {file} appears to be empty or has only a header. Skipped.")
                continue
            except Exception as e:
                logging.error(f"Error uploading {file}: {e}")
                if raw_conn:
                    raw_conn.rollback()
            finally:
                if raw_conn:
                    raw_conn.close()
                    print(f"Connection for {file} closed.")

    end = time.time()
    total_time = round((end - start) / 60, 2)
    logging.info('------------------- Ingestion completed -------------------')
    print(f"🏁 All files processed. Total time: {total_time} minutes")

# --- Run upload ---
if __name__ == '__main__':
    data_path = r'C:\Users\abhis\my_projects\project_874\vender_performance_Analysis_Project\data'
    load_data_fast(data_path, engine, chunksize=100000)