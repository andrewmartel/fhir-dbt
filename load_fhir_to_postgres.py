import json
import os
from pathlib import Path
from typing import Iterable, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "sample-bulk-fhir-datasets-10-patients"


def get_connection(dbname=None):
    """
    Read Postgres connection info from environment variables and open a connection.

    Expected env vars (you can set these in a .env file):
      PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    load_dotenv()
    db = dbname or os.getenv("PGDATABASE", "fhir")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=db,
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )
    conn.autocommit = False
    return conn


def ensure_database():
    """Create the fhir database if it does not exist (connects to 'postgres' first)."""
    load_dotenv()
    dbname = os.getenv("PGDATABASE", "fhir")
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname="postgres",
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (dbname,),
            )
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{dbname}"')
                print(f"Created database: {dbname}")
        conn.close()
    except Exception as e:
        print(f"Note: Could not ensure database (you may need to create '{dbname}' manually): {e}")


def ensure_tables(conn) -> None:
    """
    Create simple raw tables with a typed primary key and the full JSON payload.

    dbt models will build on top of these tables.
    """
    ddl_statements = [
        """
        create table if not exists raw_patient (
            patient_id text primary key,
            resource jsonb not null
        );
        """,
        """
        create table if not exists raw_immunization (
            immunization_id text primary key,
            patient_reference text not null,
            resource jsonb not null
        );
        """,
        """
        create table if not exists raw_observation (
            observation_id text primary key,
            patient_reference text not null,
            category text,
            code text,
            resource jsonb not null
        );
        """,
        """
        create table if not exists raw_diagnostic_report (
            diagnostic_report_id text primary key,
            patient_reference text not null,
            code text,
            resource jsonb not null
        );
        """,
    ]

    with conn.cursor() as cur:
        for ddl in ddl_statements:
            cur.execute(ddl)
    conn.commit()


def iter_ndjson(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_patients(conn) -> None:
    path = DATA_DIR / "Patient.000.ndjson"
    rows = []
    for obj in iter_ndjson(path):
        patient_id = obj.get("id")
        if not patient_id:
            continue
        rows.append((patient_id, json.dumps(obj)))

    with conn.cursor() as cur:
        cur.execute("truncate table raw_patient;")
        execute_batch(
            cur,
            "insert into raw_patient (patient_id, resource) values (%s, %s)",
            rows,
        )
    conn.commit()


def load_immunizations(conn) -> None:
    path = DATA_DIR / "Immunization.000.ndjson"
    rows = []
    for obj in iter_ndjson(path):
        imm_id = obj.get("id")
        if not imm_id:
            continue
        patient_ref = obj.get("patient", {}).get("reference")
        rows.append((imm_id, patient_ref, json.dumps(obj)))

    with conn.cursor() as cur:
        cur.execute("truncate table raw_immunization;")
        execute_batch(
            cur,
            """
            insert into raw_immunization (immunization_id, patient_reference, resource)
            values (%s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def load_observations(conn) -> None:
    path = DATA_DIR / "Observation.000.ndjson"
    rows = []
    for obj in iter_ndjson(path):
        obs_id = obj.get("id")
        if not obs_id:
            continue

        patient_ref = obj.get("subject", {}).get("reference")

        category_codings = (obj.get("category") or [{}])[0].get("coding") or []
        category_code = category_codings[0].get("code") if category_codings else None

        code_codings = obj.get("code", {}).get("coding") or []
        code_code = code_codings[0].get("code") if code_codings else None

        rows.append((obs_id, patient_ref, category_code, code_code, json.dumps(obj)))

    with conn.cursor() as cur:
        cur.execute("truncate table raw_observation;")
        execute_batch(
            cur,
            """
            insert into raw_observation (
                observation_id,
                patient_reference,
                category,
                code,
                resource
            )
            values (%s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def load_diagnostic_reports(conn) -> None:
    path = DATA_DIR / "DiagnosticReport.000.ndjson"
    rows = []
    for obj in iter_ndjson(path):
        dr_id = obj.get("id")
        if not dr_id:
            continue

        patient_ref = obj.get("subject", {}).get("reference")

        code_codings = obj.get("code", {}).get("coding") or []
        code_code = code_codings[0].get("code") if code_codings else None

        rows.append((dr_id, patient_ref, code_code, json.dumps(obj)))

    with conn.cursor() as cur:
        cur.execute("truncate table raw_diagnostic_report;")
        execute_batch(
            cur,
            """
            insert into raw_diagnostic_report (
                diagnostic_report_id,
                patient_reference,
                code,
                resource
            )
            values (%s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()


def main():
    conn = get_connection()
    try:
        ensure_tables(conn)
        load_patients(conn)
        load_immunizations(conn)
        load_observations(conn)
        load_diagnostic_reports(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

