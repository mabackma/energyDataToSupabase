import os
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from dictionaries import device_numbers


# Define a function to insert row into Supabase table
def insert_row(row, phase_type):
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
    # Insert data into Supabase table
    supabase.table('phase').insert(data).execute()


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

df_all = df_all.head()

# Iterate over each row in the DataFrame and insert it into the Supabase table
for row in df_all.iter_rows(named=True):
    # Insert data for each phase type (L1, L2, L3)
    for phase_type in range(1, 4):
        insert_row(row, phase_type)

    # Insert data for total phase type
    data_total = {
        'current': row['Total current'],
        'voltage': None,  # Fill with appropriate values if available
        'act_power': row['Total active power'],
        'pf': None,  # Fill with appropriate values if available
        'freq': None,  # Fill with appropriate values if available
        'total_act_energy': row['Total active energy'],
        'total_act_ret_energy': row['Total active returned energy'],
        'aprt_power': row['Total apparent power'],
        'device': device_numbers[row['meter_id']],
        'phase_type': 4,  # Total phase type
        'ts': row['ts'],
        'price_realtime': row['price']
    }
    # Insert total data into Supabase table
    supabase.table('phase').insert(data_total).execute()
