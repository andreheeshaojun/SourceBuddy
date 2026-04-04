import pandas as pd
import requests
from supabase import create_client
from dotenv import load_dotenv
import os
import re

# Load API keys from config
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Read CSV and force everything to string
df = pd.read_csv(os.path.join(os.path.dirname(__file__), "..", "CompanyData", "FilteredCompanyData.csv"), dtype=str)
df.columns = df.columns.str.strip()

# Sanitize column names: dots/spaces to underscores, lowercase
def clean_col(name):
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name

col_mapping = {col: clean_col(col) for col in df.columns}
df.rename(columns=col_mapping, inplace=True)
df = df.fillna("").replace(["nan", "NaN", "None"], "")

TABLE_NAME = "Raw CSV Data Test"

# --- Step 1: Create the table via Supabase SQL endpoint ---
print(f"Creating table '{TABLE_NAME}' if it doesn't exist...")

columns_sql = ",\n  ".join([f'"{col}" text' for col in df.columns])
create_sql = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
  id bigint generated always as identity primary key,
  {columns_sql}
);
"""

# Use the Supabase SQL API (requires service_role key)
project_ref = SUPABASE_URL.split("//")[1].split(".")[0]
sql_url = f"https://{project_ref}.supabase.co/rest/v1/rpc"

# Try executing via pg endpoint
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# Attempt 1: Use the /query endpoint (Supabase Management API style)
query_url = f"{SUPABASE_URL}/rest/v1/"
resp = requests.post(
    f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
    headers=headers,
    json={"query": create_sql}
)

if resp.status_code == 404:
    # exec_sql function doesn't exist, try creating it first
    print("exec_sql function not found. Creating helper function...")
    bootstrap_sql = """
    CREATE OR REPLACE FUNCTION exec_sql(query text) RETURNS void AS $$
    BEGIN EXECUTE query; END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;
    """
    # This also needs exec_sql to run... chicken-and-egg problem.
    # Fall back: use the Supabase database direct connection
    print("Cannot auto-create table via REST API alone.")
    print("Attempting direct database connection...")

    try:
        import psycopg2
    except ImportError:
        print("Installing psycopg2-binary...")
        os.system("pip install psycopg2-binary")
        import psycopg2

    db_password = os.getenv("SUPABASE_DB_PASSWORD")
    if not db_password:
        print("\n ERROR: SUPABASE_DB_PASSWORD not set in keys.env")
        print(f" Add this line to config/keys.env:")
        print(f" SUPABASE_DB_PASSWORD=your-database-password")
        print(f"\n Find it in Supabase Dashboard > Project Settings > Database > Connection string")
        exit(1)

    # Use the connection pooler (IPv4 compatible)
    conn = psycopg2.connect(
        host=f"aws-0-eu-west-1.pooler.supabase.com",
        port=6543,
        dbname="postgres",
        user=f"postgres.{project_ref}",
        password=db_password,
        options="-c statement_timeout=60000",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(create_sql)
    cur.close()
    conn.close()
    print(f"Table '{TABLE_NAME}' created successfully via direct DB connection!")

elif resp.status_code in (200, 204):
    print(f"Table '{TABLE_NAME}' created successfully via RPC!")
else:
    print(f"Unexpected response ({resp.status_code}): {resp.text}")
    exit(1)

# --- Step 2: Upload data ---
print(f"\nUploading {len(df)} rows to '{TABLE_NAME}'...")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

batch_size = 500
for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size].to_dict(orient="records")
    try:
        supabase.table(TABLE_NAME).insert(batch).execute()
        print(f"  Inserted {min(i+batch_size, len(df))} / {len(df)}")
    except Exception as e:
        print(f"  Error at row {i}: {e}")
        exit(1)

print("\nDone! All data uploaded successfully.")
