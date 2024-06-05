import os
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from dictionaries import device_numbers, sb_columns
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


# Define a function to insert rows into Supabase table
def insert_batch(batch_data):
    supabase.table('phase').insert(batch_data).execute()


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

'''''
# Function to update L1, L2, L3 rows
def update_row(row, phase_type):
    row['current'] = row[f'L{phase_type} current'].cast(pl.Float32) if row[f'L{phase_type} current'] is not None else None
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
'''''
# Function to update L1, L2, L3 rows
def update_row(row, phase_type):
    for col_name in ['current', 'voltage', 'active power', 'Power factor', 'frequency', 'total active energy',
                     'total active returned energy', 'apparent power']:
        key = f'L{phase_type} {col_name}'
        value = row[key]
        if value is not None:
            row[sb_columns[col_name]] = np.float32(value)  # Convert to float
        else:
            row[sb_columns[col_name]] = None
    row['device'] = device_numbers[row['meter_id']]
    row['phase_type'] = phase_type
    row['ts'] = row['ts_orig']
    row['price_realtime'] = np.float32(row['price'])
    return row

'''''
# Function to update row for total data
def update_total_row(row):
    row['current'] = row['Total current']
    row['voltage'] = None
    row['act_power'] = row['Total active power']
    row['pf'] = None
    row['freq'] = None
    row['total_act_energy'] = row['Total active energy']
    row['total_act_ret_energy'] = row['Total active returned energy']
    row['aprt_power'] = row['Total apparent power']
    row['device'] = device_numbers[row['meter_id']]
    row['phase_type'] = 4  # Total phase type
    row['ts'] = row['ts_orig']
    row['price_realtime'] = row['price']
    return row
'''''


# Function to update row for total data
def update_total_row(row):
    for col_name in ['Total current', 'Total active power', 'Total active energy',
                     'Total active returned energy', 'Total apparent power']:
        key = f'{col_name}'
        value = row[key]
        if value is not None:
            row[sb_columns[col_name]] = np.float32(value)  # Convert to float
        else:
            row[sb_columns[col_name]] = None
    row['voltage'] = None
    row['pf'] = None
    row['freq'] = None
    row['device'] = device_numbers[row['meter_id']]
    row['phase_type'] = 4  # Total phase type
    row['ts'] = row['ts_orig']
    row['price_realtime'] = np.float32(row['price'])
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
df_all = df_all.head(1000)

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
'''''
# Initialize an empty list for updated rows
updated_rows = []

# Iterate over each row in the DataFrame
for row in df_all.iter_rows(named=True):

    # Update the row for each phase type (1, 2, 3)
    for phase_type in range(1, 4):
        updated_row = update_row(dict(row), phase_type)
        updated_rows.append(updated_row)

    # Update the row for the total values
    updated_total_row = update_total_row(dict(row))
    updated_rows.append(updated_total_row)

# Replace the dataframe with a new dataframe containing the updated rows
df_all = pl.DataFrame(updated_rows)
'''''
# Extract column names and data types from df_all
column_types = {col: df_all[col].dtype for col in df_all.columns}

# Create an empty DataFrame with the same column names and data types as df_all
df_updated = pl.DataFrame({col: pl.Series([], dtype=column_types[col]) for col in df_all.columns})
def insert_rows(df, updated_rows):
    df_new = pl.DataFrame(updated_rows)
    print('---')
    df.vstack(df_new)

batch_size = 1000
updated_rows = []
futures = []
with ThreadPoolExecutor(max_workers=16) as executor:  # Set to 16 for Ryzen 5800X
    for row in df_all.iter_rows(named=True):
        # Update the row for each phase type (1, 2, 3)
        for phase_type in range(1, 4):
            updated_row = update_row(dict(row), phase_type)
            updated_rows.append(updated_row)

        # Update the row for the total values
        updated_total_row = update_total_row(dict(row))
        updated_rows.append(updated_total_row)

        # If batch_data has reached the batch_size, submit it as a task
        if len(updated_rows) >= batch_size:
            futures.append(executor.submit(insert_rows, df_updated, updated_rows.copy()))
            updated_rows = []

    # If there is any remaining data in batch_data, submit it as the final batch
    if updated_rows:
        futures.append(executor.submit(insert_rows, df_updated, updated_rows.copy()))

    # Process the results from the batch submissions
    for future in as_completed(futures):
        future.result()  # This will raise an exception if the upload failed

# Remove unnecessary columns
columns_to_remove = ['L1 current', 'L1 voltage', 'L1 active power', 'L1 Power factor', 'L1 frequency',
                     'L1 total active energy', 'L1 total active returned energy', 'L1 apparent power',
                     'L2 current', 'L2 voltage', 'L2 active power', 'L2 Power factor', 'L2 frequency',
                     'L2 total active energy', 'L2 total active returned energy', 'L2 apparent power',
                     'L3 current', 'L3 voltage', 'L3 active power', 'L3 Power factor', 'L3 frequency',
                     'L3 total active energy', 'L3 total active returned energy', 'L3 apparent power',
                     'meter_id', 'ts_orig', 'price', 'Total current', 'Total active power',
                     'Total active energy', 'Total active returned energy', 'Total apparent power']
#df_all = df_all.drop(columns_to_remove)
df_updated = df_updated.drop(columns_to_remove)

# Write files from the dataframe
dirpath = Path(".")
path_parquet = dirpath / "supabase_data.parquet"
df_updated.write_parquet(path_parquet)
path_csv = dirpath / "supabase_data.csv"
df_updated.write_csv(path_csv, separator=";")

print("start")
# Process and upload data
#process_and_upload(df_all, batch_size=1000)
print("end")
