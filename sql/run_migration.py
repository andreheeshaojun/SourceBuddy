"""Run the metadata-to-columns migration against Supabase via psycopg2."""
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

sql_path = os.path.join(os.path.dirname(__file__), "migrate_metadata_to_columns.sql")
with open(sql_path) as f:
    # Strip comment-only lines starting with --
    lines = [l for l in f if not l.strip().startswith("--")]
    sql = "".join(lines)

print("Connecting to Supabase PostgreSQL...")
conn = psycopg2.connect(conn_str)
conn.autocommit = True
cur = conn.cursor()

# Execute each UPDATE statement separately and report row counts
statements = [s.strip() for s in sql.split(";") if s.strip()]
for stmt in statements:
    print(f"\nExecuting: {stmt[:80]}...")
    cur.execute(stmt)
    print(f"  Rows updated: {cur.rowcount}")

# Verification query
print("\n--- Verification ---")
cur.execute("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(*) FILTER (WHERE pipeline_status IS NOT NULL) AS pipeline_status_filled,
        COUNT(*) FILTER (WHERE filing_format IS NOT NULL) AS filing_format_filled,
        COUNT(*) FILTER (WHERE sic_code_1 IS NOT NULL) AS sic_code_1_filled,
        COUNT(*) FILTER (WHERE company_status IS NOT NULL) AS company_status_filled,
        COUNT(*) FILTER (WHERE accounts_category IS NOT NULL) AS accounts_category_filled
    FROM companies;
""")
cols = [desc[0] for desc in cur.description]
row = cur.fetchone()
for col, val in zip(cols, row):
    print(f"  {col}: {val}")

cur.close()
conn.close()
print("\nMigration complete.")
