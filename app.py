import os
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from dictionaries import device_numbers
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import math
import glob
from join_files import sort_files_in_list, check_file_lengths, join_csv_files
from write_files import write_csv_file
import time
'''''
# Define a function to insert rows into Supabase table
def insert_batch(batch_data):
    supabase.table('phase').insert(batch_data).execute()

def prepare_data(row, phase_type):
    return {
        'current': row[f'L{phase_type} current'],
        'voltage': row[f'L{phase_type} voltage'],
        'act_power': row[f'L{phase_type} active power'],
        'pf': row[f'L{phase_type} Power factor'],
        'freq': row[f'L{phase_type} frequency'],
        'total_act_energy': row[f'L{phase_type} total active energy'],
        'total_act_ret_energy': row[f'L{phase_type} total active returned energy'],
        'aprt_power': row[f'L{phase_type} apparent power'],
        'device': device_numbers[row['meter_id']],
        'phase_type': phase_type,
        'ts': row['ts'],
        'price_realtime': row['price']
    }

def prepare_total_data(row):
    return {
        'current': row['Total current'],
        'voltage': 0,  # Assuming 0 instead of None for batch insert consistency
        'act_power': row['Total active power'],
        'pf': 0,  # Assuming 0 instead of None for batch insert consistency
        'freq': 0,  # Assuming 0 instead of None for batch insert consistency
        'total_act_energy': row['Total active energy'],
        'total_act_ret_energy': row['Total active returned energy'],
        'aprt_power': row['Total apparent power'],
        'device': device_numbers[row['meter_id']],
        'phase_type': 4,  # Total phase type
        'ts': row['ts'],
        'price_realtime': row['price']
    }

def process_and_upload(df_all, batch_size=1000):
    batch_data = []
    futures = []
    with ThreadPoolExecutor(max_workers=16) as executor:  # Set to 16 for Ryzen 5800X
        for row in df_all.iter_rows(named=True):
            for phase_type in range(1, 4):
                batch_data.append(prepare_data(row, phase_type))
            batch_data.append(prepare_total_data(row))

            if len(batch_data) >= batch_size:
                futures.append(executor.submit(insert_batch, batch_data.copy()))
                batch_data = []

        if batch_data:
            futures.append(executor.submit(insert_batch, batch_data.copy()))

        for future in as_completed(futures):
            future.result()  # This will raise an exception if the upload failed

load_dotenv()
url = os.getenv('SUPABASE_URL')
api_key = os.getenv('SUPABASE_API_KEY')

# Initialize Supabase client
supabase = create_client(url, api_key)

df_all = pl.read_parquet("all_data_with_price.parquet")

# Modify columns
df_all = df_all.with_columns(pl.col("L3 total active returned energy").alias("L2 total active returned energy"))
df_all = df_all.with_columns(pl.col("L3 total active returned energy_right").alias("L3 total active returned energy"))
df_all = df_all.drop("L3 total active returned energy_right")

# Convert timestamp column to string format
df_all = df_all.with_columns(pl.col("ts").dt.strftime('%Y-%m-%d %H:%M:%S').alias("ts"))

# Remove the head() call to process the entire DataFrame
df_all = df_all.head(100000)
print("start")
# Process and upload data
process_and_upload(df_all, batch_size=1000)
print("end")
'''''


# Define a function to insert rows into Supabase table
def insert_batch(batch_data):
    max_retries = 5
    delay = 3
    attempt = 0
    while attempt < max_retries:
        try:
            supabase.table('phase').insert(batch_data).execute()
            return  # Success, exit the function
        except Exception as e:
            print(f"Error inserting batch (attempt {attempt + 1}): {e}")
            attempt += 1
            if attempt < max_retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise  # Raise the exception if all retries fail


# Function to upload data in batches
def process_and_upload(df_new, batch_size=1000):
    batch_data = []
    futures = []
    with ThreadPoolExecutor(max_workers=16) as executor:  # Set to 16 for Ryzen 5800X
        for row in df_new.iter_rows(named=True):
            batch_data.append(row)

            # If batch_data has reached the batch_size, submit it as a task
            if len(batch_data) >= batch_size:
                futures.append(executor.submit(insert_batch, batch_data.copy()))
                batch_data = []

        # If there is any remaining data in batch_data, submit it as the final batch
        if batch_data:
            futures.append(executor.submit(insert_batch, batch_data.copy()))

        # Process the results from the batch submissions
        for future in as_completed(futures):
            future.result()  # This will raise an exception if the upload failed


load_dotenv()
url = os.getenv('SUPABASE_URL')
api_key = os.getenv('SUPABASE_API_KEY')

# Initialize Supabase client
supabase = create_client(url, api_key)
'''''
df_all = pl.read_parquet("all_data_with_price.parquet")

# Modify columns
df_all = df_all.with_columns(pl.col("ts").alias("ts_orig"))
df_all = df_all.drop('ts')
df_all = df_all.with_columns(pl.col("L3 total active returned energy").alias("L2 total active returned energy"))
df_all = df_all.with_columns(pl.col("L3 total active returned energy_right").alias("L3 total active returned energy"))
df_all = df_all.drop("L3 total active returned energy_right")

# Convert timestamp column to string format
df_all = df_all.with_columns(pl.col("ts_orig").dt.strftime('%Y-%m-%d %H:%M:%S').alias("ts_orig"))

# Define new columns and their data types
new_columns = {
    'current': pl.Float32,
    'voltage': pl.Float32,
    'act_power': pl.Float32,
    'pf': pl.Float32,
    'freq': pl.Float32,
    'total_act_energy': pl.Float32,
    'total_act_ret_energy': pl.Float32,
    'aprt_power': pl.Float32,
    'device': pl.Int32,
    'phase_type': pl.Int32,
    'ts': pl.Utf8,
    'price_realtime': pl.Float32
}

# Add all new columns to the dataframe and initialize them with None values
for col_name, col_type in new_columns.items():
    df_all = df_all.with_columns(pl.lit(None).cast(col_type).alias(col_name))

# Create CSV files from 1 000 000 rows of the dataframe (Creates CSV files containing 4 000 000 rows)
amount_of_files = math.ceil(len(df_all) / 1000000)
for i in range(amount_of_files):
    print(f"writing file #{i + 1}")
    start_idx = i * 1000000
    end_idx = min((i + 1) * 1000000, len(df_all))
    df_slice = df_all.slice(start_idx, end_idx - start_idx)
    write_csv_file(df_slice, i + 1)

# Join all the CSV files into one final CSV file
all_files = glob.glob("./csv_files/*.csv")
sorted_files = sort_files_in_list(all_files)
check_file_lengths(sorted_files)
join_csv_files(sorted_files)
'''''
# Create a dataframe from the final CSV
df = pl.read_csv("data_files/supabase_data.csv", separator=";")
print(df.head())
print(f'Length: {df.shape[0]}')

# Create a parquet file
#df.write_parquet("./data_files/supabase_data_parquet.parquet")

# Upload the data to Supabase
df = df.head(20000000)
print("start")
process_and_upload(df, batch_size=1000)
print("end")
