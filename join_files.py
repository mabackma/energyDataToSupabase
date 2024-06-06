import shutil
import polars as pl
import os


# Joins all CSV files to a single CSV file and saves the file.
def join_csv_files(all_files):
    os.makedirs('data_files', exist_ok=True)
    with open('data_files/supabase_data.csv', 'wb') as outfile:
        for i, fname in enumerate(all_files):
            with open(fname, 'rb') as infile:
                if i != 0:
                    infile.readline()  # Throw away header on all but first file
                # Block copy rest of file from input to output without parsing
                shutil.copyfileobj(infile, outfile)
                print(fname + " has been imported.")


# Sorts CSV files
def sort_files_in_list(files):
    new_list = []
    for file in files:
        file = file.replace("./csv_files\\supabase_data", "")
        file = file.replace(".csv", "")
        file = int(file)
        new_list.append(file)
    new_list = sorted(new_list)

    sorted_list = []
    for file in new_list:
        file = f"./csv_files\\supabase_data{file}.csv"
        sorted_list.append(file)

    return sorted_list


# Checks lengths of CSV files (they should all be 4 000 000 rows)
def check_file_lengths(files):
    print("Checking file lengths...")
    for file in files:
        df = pl.read_csv(file)
        print(len(df))

