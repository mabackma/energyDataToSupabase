import os
from dotenv import load_dotenv
from supabase import create_client
import datetime

load_dotenv()
url = os.getenv('SUPABASE_URL')
api_key = os.getenv('SUPABASE_API_KEY')

# Initialize Supabase client
supabase = create_client(url, api_key)

# TODO Delete data before 2024-5-12
cutoff_date = datetime.datetime(2024, 3, 26)
cutoff_date_str = str(cutoff_date)
table_name = 'phase'
timestamp_field = 'ts'
response = supabase.table(table_name).delete().lt(timestamp_field, cutoff_date_str).execute()





