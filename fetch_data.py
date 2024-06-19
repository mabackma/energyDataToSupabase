import os
from dotenv import load_dotenv
from supabase import create_client
import datetime
import polars as pl

load_dotenv()
url = os.getenv('SUPABASE_URL')
api_key = os.getenv('SUPABASE_API_KEY')

# Initialize Supabase client
supabase = create_client(url, api_key)

start_date = datetime.datetime(2023, 1, 1)
start_date_str = str(start_date)
table_name = 'phase'
timestamp_field = 'ts'
# Fetch the data
response = supabase.table(table_name).select('*').lt(timestamp_field, start_date_str).execute()

data = response.data
# Convert the data to a DataFrame
df = pl.DataFrame(data)
print(df.tail(10))
print(df.height)