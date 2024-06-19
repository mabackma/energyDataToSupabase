import os

import polars as pl
from dotenv import load_dotenv

# Create a dataframe from CSV
#df = pl.read_csv("from_supabase.csv", separator=",")
#print(df.head())
#print(f'Length: {df.shape[0]}')


load_dotenv()
password = os.getenv('SUPABASE_PASSWORD')

uri = f"postgres://postgres.virzcqacmgnbkahkckjb:{password}@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
query = "SELECT * FROM phase"

df = pl.read_database_uri(query=query, uri=uri)
print(df.head())
print(f'Length: {df.shape[0]}')
