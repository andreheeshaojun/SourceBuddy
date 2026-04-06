"""
Read-only checks against live Supabase to verify the metadata-to-columns migration.
Does NOT modify any data.
"""
import os
import psycopg2
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
PROJECT_REF = SUPABASE_URL.replace("https://", "").split(".")[0]

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host="aws-0-eu-west-1.pooler.supabase.com",
        port=6543,
        dbname="postgres",
        user=f"postgres.{PROJECT_REF}",
        password=DB_PASSWORD,
    )


def query_one(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = get_pg_conn()
    cur = conn.cursor()

    # ── 1. Total rows ───────────────────────────────────────���────────────────
    section("1. Row counts")
    total = query_one(cur, "SELECT COUNT(*) FROM companies")
    print(f"  Total rows: {total}")

    # ── 2-6. Column population checks ────────────────────────────────────────
    section("2-6. Column population")
    checks = [
        ("pipeline_status",   total),
        ("company_status",    total),
        ("accounts_category", total),
        ("filing_format",     0),
        ("last_accounts_date", 0),
    ]
    all_pass = True
    for col, expected in checks:
        filled = query_one(cur, f"SELECT COUNT(*) FROM companies WHERE {col} IS NOT NULL")
        status = "PASS" if filled == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(f"  {col}: {filled}/{total} filled  (expected {expected}/{total})  [{status}]")

    # ── 7-9. Side-by-side comparison on 5 random rows ────────────────────────
    section("7-9. Side-by-side: typed column vs metadata (5 random rows)")
    cur.execute("""
        SELECT
            company_number,
            company_name,
            pipeline_status,
            metadata->'pipeline'->>'status'   AS meta_pipeline_status,
            company_status,
            metadata->>'company_status'        AS meta_company_status,
            accounts_category,
            metadata->>'accounts_category'     AS meta_accounts_category
        FROM companies
        ORDER BY random()
        LIMIT 5
    """)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    for row in rows:
        r = dict(zip(cols, row))
        cn = r["company_number"]
        name = r["company_name"]
        print(f"\n  {cn} ({name})")

        comparisons = [
            ("pipeline_status",   r["pipeline_status"],   r["meta_pipeline_status"]),
            ("company_status",    r["company_status"],     r["meta_company_status"]),
            ("accounts_category", r["accounts_category"],  r["meta_accounts_category"]),
        ]
        for field, typed_val, meta_val in comparisons:
            match = "MATCH" if typed_val == meta_val else "MISMATCH"
            if match == "MISMATCH":
                all_pass = False
            print(f"    {field:<20}  typed={typed_val!s:<15}  metadata={meta_val!s:<15}  [{match}]")

    # ── 10. New read path test via supabase-py ───────────────────────────────
    section("10. New read path: .eq('pipeline_status', 'pending')")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    result = (
        supabase.table("companies")
        .select("company_number", count="exact")
        .eq("pipeline_status", "pending")
        .limit(1)
        .execute()
    )
    pending_count = result.count
    print(f"  Pending companies via new read path: {pending_count}")
    if pending_count is not None and pending_count > 0:
        print(f"  [PASS] Query works and found {pending_count} rows")
    elif pending_count == 0:
        print(f"  [PASS] Query works (0 pending — all may have been processed)")
    else:
        print(f"  [FAIL] Query returned None — check supabase-py client")
        all_pass = False

    # ── Summary ──────────────────────────────────────────────────────────────
    section("Summary")
    if all_pass:
        print("  All checks PASSED.")
    else:
        print("  Some checks FAILED — review output above.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
