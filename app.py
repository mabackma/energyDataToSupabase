import os
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from dictionaries import device_numbers


# Define a function to insert rows into Supabase table
def insert_batch(batch_data):
    supabase.table('phase').insert(batch_data).execute()


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

# Fill nan values with 0
df_all = df_all.fill_null(0)

# Limit to a smaller dataset for this example
df_all = df_all.head(10000)

# Collect data for batch insert
batch_data = []
batch_size = 1000

for row in df_all.iter_rows(named=True):
    # Insert data for each phase type (L1, L2, L3)
    for phase_type in range(1, 4):
        data = {
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
        batch_data.append(data)

    # Insert data for total phase type
    data_total = {
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
    batch_data.append(data_total)

    # If batch size is reached, insert the batch and reset
    if len(batch_data) >= batch_size:
        insert_batch(batch_data)
        batch_data = []

# Insert any remaining data
if batch_data:
    insert_batch(batch_data)