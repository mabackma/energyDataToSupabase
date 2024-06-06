import os
import polars as pl
from dictionaries import device_numbers
from pathlib import Path
import shutil
import math


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


def write_csv_file(df, name):
    # Initialize an empty list for updated rows
    updated_rows = []

    # Iterate over each row in the DataFrame
    for row in df.iter_rows(named=True):

        # Update the row for each phase type (1, 2, 3)
        for phase_type in range(1, 4):
            updated_row = update_row(dict(row), phase_type)
            updated_rows.append(updated_row)

        # Update the row for the total values
        updated_total_row = update_total_row(dict(row))
        updated_rows.append(updated_total_row)

    # Replace the dataframe with a new dataframe containing the updated rows
    df = pl.DataFrame(updated_rows, infer_schema_length=1000)

    # Remove unnecessary columns
    columns_to_remove = ['L1 current', 'L1 voltage', 'L1 active power', 'L1 Power factor', 'L1 frequency',
                         'L1 total active energy', 'L1 total active returned energy', 'L1 apparent power',
                         'L2 current', 'L2 voltage', 'L2 active power', 'L2 Power factor', 'L2 frequency',
                         'L2 total active energy', 'L2 total active returned energy', 'L2 apparent power',
                         'L3 current', 'L3 voltage', 'L3 active power', 'L3 Power factor', 'L3 frequency',
                         'L3 total active energy', 'L3 total active returned energy', 'L3 apparent power',
                         'meter_id', 'ts_orig', 'price', 'Total current', 'Total active power',
                         'Total active energy', 'Total active returned energy', 'Total apparent power']
    df = df.drop(columns_to_remove)

    # Write CSV file from the dataframe
    dirpath = Path("./csv_files")
    os.makedirs(dirpath, exist_ok=True)
    path_csv = dirpath / f"supabase_data{name}.csv"
    df.write_csv(path_csv, separator=";")

def join_csv_files(all_files):
    with open('joined_files.csv', 'wb') as outfile:
        for i, fname in enumerate(all_files):
            with open(fname, 'rb') as infile:
                print(f"index is {i}")
                if i != 0:
                    infile.readline()  # Throw away header on all but first file
                # Block copy rest of file from input to output without parsing
                shutil.copyfileobj(infile, outfile)
                print(fname + " has been imported.")


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

amount_of_files = math.ceil(len(df_all) / 500000)

for i in range(amount_of_files):
    print(f"writing file #{i + 1}")
    start_idx = i * 500000
    end_idx = start_idx + 500000
    df_slice = df_all.slice(start_idx, end_idx)
    write_csv_file(df_slice, i + 1)



