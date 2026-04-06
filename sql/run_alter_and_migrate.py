"""Run ALTER TABLE then backfill migration against Supabase."""
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

print("Connecting to Supabase PostgreSQL...")
conn = psycopg2.connect(conn_str)
conn.autocommit = True
cur = conn.cursor()

# ── Step 1: ALTER TABLE ─────────────────────────────────────��────────────────
print("\n=== Step 1: Adding columns ===")
alter_statements = [
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS pipeline_status TEXT",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS filing_format TEXT",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_accounts_date DATE",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_status TEXT",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS accounts_category TEXT",
]
for stmt in alter_statements:
    col = stmt.split("IF NOT EXISTS ")[1].split(" ")[0]
    cur.execute(stmt)
    print(f"  Added: {col}")

# ── Step 2: Backfill from metadata ───────────────────────────────────────────
print("\n=== Step 2: Backfilling from metadata ===")
backfill_statements = [
    ("pipeline_status",   "metadata->'pipeline'->>'status'"),
    ("filing_format",     "metadata->>'filing_format'"),
    ("last_accounts_date","(metadata->>'last_accounts_date')::date"),
    ("company_status",    "metadata->>'company_status'"),
    ("accounts_category", "metadata->>'accounts_category'"),
]
for col, expr in backfill_statements:
    sql = f"""
        UPDATE companies
        SET {col} = {expr}
        WHERE {col} IS NULL
          AND {expr.replace('::date', '')} IS NOT NULL
    """
    cur.execute(sql)
    print(f"  {col}: {cur.rowcount} rows backfilled")

# ── Step 3: Verify ───────────────────────────────────────────────────────────
print("\n=== Verification ===")
cur.execute("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(*) FILTER (WHERE pipeline_status IS NOT NULL) AS pipeline_status_filled,
        COUNT(*) FILTER (WHERE filing_format IS NOT NULL) AS filing_format_filled,
        COUNT(*) FILTER (WHERE last_accounts_date IS NOT NULL) AS last_accounts_date_filled,
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
print("\nDone.")
