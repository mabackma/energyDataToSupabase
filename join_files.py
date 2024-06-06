import shutil
import glob
import polars as pl


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


def check_file_lengths(files):
    for file in files:
        df = pl.read_csv(file)
        print(len(df))


all_files = glob.glob("./csv_files/*.csv")
sorted_files = sort_files_in_list(all_files)
check_file_lengths(sorted_files)
join_csv_files(sorted_files)

df = pl.read_csv("joined_files.csv", separator=";")
print(df.head())
print(f'Length: {df.shape[0]}')
