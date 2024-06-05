import os
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from dictionaries import device_numbers
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    supabase.table('phase').insert(batch_data).execute()


def process_and_upload(df_new, batch_size=1000):
    batch_data = []
    futures = []
    with ThreadPoolExecutor(max_workers=16) as executor:  # Set to 16 for Ryzen 5800X
        for row in df_new.iter_rows(named=True):
            batch_data.append(row)
            if len(batch_data) >= batch_size:
                futures.append(executor.submit(insert_batch, batch_data.copy()))
                batch_data = []
        if batch_data:
            futures.append(executor.submit(insert_batch, batch_data.copy()))
        for future in as_completed(futures):
            future.result()  # This will raise an exception if the upload failed


# Function to update L1, L2, L3 rows
def update_row(row, phase_type):
    row['current'] = row[f'L{phase_type} current']
    row['voltage'] = row[f'L{phase_type} voltage']
    row['act_power'] = row[f'L{phase_type} active power']
    row['pf'] = row[f'L{phase_type} Power factor']
    row['freq'] = row[f'L{phase_type} frequency']
    row['total_act_energy'] = row[f'L{phase_type} total active energy']
    row['total_act_ret_energy'] = row[f'L{phase_type} total active returned energy']
    row['aprt_power'] = row[f'L{phase_type} apparent power']
    row['device'] = device_numbers[row['meter_id']]
    row['phase_type'] = phase_type
    row['ts'] = row['ts_orig']
    row['price_realtime'] = row['price']
    return row


# Function to update row for total data
def update_total_row(row):
    row['current'] = row['Total current']
    row['voltage'] = 0  # Assuming 0 instead of None for batch insert consistency
    row['act_power'] = row['Total active power']
    row['pf'] = 0  # Assuming 0 instead of None for batch insert consistency
    row['freq'] = 0  # Assuming 0 instead of None for batch insert consistency
    row['total_act_energy'] = row['Total active energy']
    row['total_act_ret_energy'] = row['Total active returned energy']
    row['aprt_power'] = row['Total apparent power']
    row['device'] = device_numbers[row['meter_id']]
    row['phase_type'] = 4  # Total phase type
    row['ts'] = row['ts_orig']
    row['price_realtime'] = row['price']
    return row


load_dotenv()
url = os.getenv('SUPABASE_URL')
api_key = os.getenv('SUPABASE_API_KEY')

# Initialize Supabase client
supabase = create_client(url, api_key)

df_all = pl.read_parquet("all_data_with_price.parquet")

# Modify columns
df_all = df_all.with_columns(pl.col("ts").alias("ts_orig"))
df_all = df_all.drop('ts')
df_all = df_all.with_columns(pl.col("L3 total active returned energy").alias("L2 total active returned energy"))
df_all = df_all.with_columns(pl.col("L3 total active returned energy_right").alias("L3 total active returned energy"))
df_all = df_all.drop("L3 total active returned energy_right")

# Convert timestamp column to string format
df_all = df_all.with_columns(pl.col("ts_orig").dt.strftime('%Y-%m-%d %H:%M:%S').alias("ts_orig"))

# Remove the head() call to process the entire DataFrame
df_all = df_all.head(100000)

# Add new columns to the DataFrame
new_columns = {
    'current': pl.Float64,
    'voltage': pl.Float64,
    'act_power': pl.Float64,
    'pf': pl.Float64,
    'freq': pl.Float64,
    'total_act_energy': pl.Float64,
    'total_act_ret_energy': pl.Float64,
    'aprt_power': pl.Float64,
    'device': pl.Int32,
    'phase_type': pl.Int32,
    'ts': pl.Utf8,
    'price_realtime': pl.Float64
}

for col_name, col_type in new_columns.items():
    df_all = df_all.with_columns(pl.lit(None).cast(col_type).alias(col_name))

updated_rows = []
for row in df_all.iter_rows(named=True):
    for phase_type in range(1, 4):
        updated_row = update_row(dict(row), phase_type)
        updated_rows.append(updated_row)
    updated_total_row = update_total_row(dict(row))
    updated_rows.append(updated_total_row)

df_all = pl.DataFrame(updated_rows)

columns_to_remove = ['L1 current', 'L1 voltage', 'L1 active power', 'L1 Power factor', 'L1 frequency',
                     'L1 total active energy', 'L1 total active returned energy', 'L1 apparent power',
                     'L2 current', 'L2 voltage', 'L2 active power', 'L2 Power factor', 'L2 frequency',
                     'L2 total active energy', 'L2 total active returned energy', 'L2 apparent power',
                     'L3 current', 'L3 voltage', 'L3 active power', 'L3 Power factor', 'L3 frequency',
                     'L3 total active energy', 'L3 total active returned energy', 'L3 apparent power',
                     'meter_id', 'ts_orig', 'price', 'Total current', 'Total active power',
                     'Total active energy', 'Total active returned energy', 'Total apparent power']
df_all = df_all.drop(columns_to_remove)

print("start")
# Process and upload data
process_and_upload(df_all, batch_size=1000)
print("end")