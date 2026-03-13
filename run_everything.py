"""
Run the full assignment with Postgres: ensure DB, load data, dbt run/test, then run the 5 assignment queries.
Requires: .env with Postgres credentials, and packages: psycopg2-binary, python-dotenv, dbt-postgres.
"""
import os
import subprocess
import sys
from pathlib import Path

# Load env before importing loader (so get_connection sees it)
from dotenv import load_dotenv
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)


def run(cmd, env=None, check=True):
    e = os.environ.copy()
    if env:
        e.update(env)
    return subprocess.run(cmd, shell=True, env=e, cwd=BASE_DIR, check=check)


def main():
    print("=" * 60)
    print("Step 1: Ensure database exists")
    print("=" * 60)
    from load_fhir_to_postgres import ensure_database, get_connection, ensure_tables
    from load_fhir_to_postgres import load_patients, load_immunizations, load_observations, load_diagnostic_reports

    try:
        ensure_database()
    except Exception as e:
        if "password authentication failed" in str(e) or "FATAL" in str(e):
            print("\n*** Postgres password didn't work. Edit the .env file in this folder and set PGPASSWORD to your real Postgres password, then run this script again. ***")
            raise SystemExit(1)
        raise

    print("\n" + "=" * 60)
    print("Step 2: Load FHIR NDJSON into Postgres")
    print("=" * 60)
    try:
        conn = get_connection()
    except Exception as e:
        if "password authentication failed" in str(e) or "FATAL" in str(e):
            print("\n*** Postgres password didn't work. Edit the .env file in this folder and set PGPASSWORD to your real Postgres password, then run this script again. ***")
            raise SystemExit(1)
        raise
    try:
        ensure_tables(conn)
        load_patients(conn)
        load_immunizations(conn)
        load_observations(conn)
        load_diagnostic_reports(conn)
        print("Loaded raw_patient, raw_immunization, raw_observation, raw_diagnostic_report.")
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("Step 3: dbt run")
    print("=" * 60)
    # Pass Postgres password to dbt from .env (PGPASSWORD) so users only set credentials in .env
    dbt_env = {
        "DBT_PROFILES_DIR": str(BASE_DIR),
        "DBT_PASSWORD": os.getenv("PGPASSWORD", ""),
    }
    try:
        run("dbt run", env=dbt_env)
    except subprocess.CalledProcessError:
        run(f'"{sys.executable}" -m dbt run', env=dbt_env)

    print("\n" + "=" * 60)
    print("Step 4: dbt test")
    print("=" * 60)
    try:
        run("dbt test", env=dbt_env)
    except subprocess.CalledProcessError:
        run(f'"{sys.executable}" -m dbt test', env=dbt_env)

    print("\n" + "=" * 60)
    print("Step 5: Run assignment queries and print results")
    print("=" * 60)
    import psycopg2

    conn = get_connection()
    try:
        cur = conn.cursor()
        # dbt default: schema = target_schema + "_" + custom_schema (e.g. public_fhir_marts)
        cur.execute("""
            SELECT table_schema FROM information_schema.tables
            WHERE table_name = 'mart_immunization_stats' LIMIT 1
        """)
        row = cur.fetchone()
        mart_schema = (row[0] if row else "public_fhir_marts")

        # Q1
        cur.execute(f'SELECT patients_with_influenza_vaccine FROM "{mart_schema}".mart_immunization_stats')
        q1 = cur.fetchone()[0]
        print("\nQ1: How many patients had the influenza vaccine?")
        print(f"    Answer: {q1}")

        # Q2
        cur.execute(f"""
            SELECT (j->>'vaccine_name') AS vaccine_name, (j->>'administrations')::int AS administrations
            FROM "{mart_schema}".mart_immunization_stats, jsonb_array_elements(top_5_vaccines) AS j
            ORDER BY administrations DESC, vaccine_name
        """)
        rows = cur.fetchall()
        print("\nQ2: Top 5 most common vaccines?")
        for i, (name, count) in enumerate(rows, 1):
            print(f"    {i}. {name}: {count}")

        # Q3
        cur.execute(f'SELECT avg_vital_signs_per_patient FROM "{mart_schema}".mart_vital_signs_per_patient')
        q3 = cur.fetchone()[0]
        print("\nQ3: Average number of vital signs per patient?")
        print(f"    Answer: {q3}")

        # Q4
        cur.execute(f"""
            SELECT patient_id, diagnostic_report_id, effective_at, code
            FROM "{mart_schema}".mart_recent_cholesterol_report_per_patient
            ORDER BY patient_id
        """)
        rows = cur.fetchall()
        print("\nQ4: Most recent report with cholesterol lab per patient?")
        for r in rows:
            print(f"    Patient {r[0]}: report={r[1]}, effective={r[2]}, code={r[3]}")

        # Q5
        cur.execute(f'SELECT * FROM "{mart_schema}".mart_patient_profile ORDER BY patient_id LIMIT 1')
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        print("\nQ5: Two facts about one patient (first in list)?")
        print(f"    {dict(zip(cols, row))}")
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("Full assignment run with database complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
