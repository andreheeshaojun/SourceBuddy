"""Check actual columns on the companies table."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
PROJECT_REF = SUPABASE_URL.replace("https://", "").split(".")[0]

conn_str = (
    f"host=aws-0-eu-west-1.pooler.supabase.com "
    f"port=6543 "
    f"dbname=postgres "
    f"user=postgres.{PROJECT_REF} "
    f"password={DB_PASSWORD}"
)

conn = psycopg2.connect(conn_str)
cur = conn.cursor()
cur.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'companies' AND table_schema = 'public'
    ORDER BY ordinal_position;
""")
print(f"{'Column':<35} {'Type':<25} {'Nullable'}")
print("-" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<35} {row[1]:<25} {row[2]}")
cur.close()
conn.close()
