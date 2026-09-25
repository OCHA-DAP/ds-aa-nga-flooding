"""One-off: bootstrap the PROD Postgres for the monitoring pipeline.

The dev DB (where projects.* monitoring tables lived) lost public network
access on 2026-09-22; prod had no `projects` schema at all. Run this ONCE via
a Databricks job (the Job Compute policy injects the prod write creds) before
the monitoring pipeline's first prod run. Idempotent. It

1. creates the `projects` schema (needs CREATE on the database — the
   dbwriter role does not have it, so a DB admin may have to run the schema
   statements; the script then carries on if the schema already exists);
2. creates `projects.ds_aa_nga_flooding_monitoring` with the same columns and the
   `<table>_unique` constraint the dev table had. The constraint is what
   `stratus.postgres_upsert` (ON CONFLICT ON CONSTRAINT) needs — a table
   created implicitly by pandas `to_sql` has none and the first upsert fails;
3. with --backfill, upserts the dev history exported by
   scripts/export_dev_monitoring_table.py (a parquet in the dev blob, which
   Databricks can still reach) into the prod table.

    python scripts/init_prod_projects_schema.py --mode prod [--backfill]
"""

import argparse

import ocha_stratus as stratus
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

TABLE = "ds_aa_nga_flooding_monitoring"
BLOB_PARQUET = "ds-aa-nga-flooding/migration/projects_ds_aa_nga_flooding_monitoring.parquet"

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS projects;
ALTER SCHEMA projects OWNER TO dbwriter;
GRANT USAGE, CREATE ON SCHEMA projects TO dbwriter;
GRANT USAGE ON SCHEMA projects TO dbreader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbwriter IN SCHEMA projects
    GRANT SELECT ON TABLES TO dbreader;
ALTER DEFAULT PRIVILEGES IN SCHEMA projects
    GRANT SELECT ON TABLES TO dbreader;
"""

# Mirrors the dev table (KB infrastructure/db-schema-dev.md) incl. the unique
# constraint postgres_upsert relies on.
TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS projects.{TABLE} (
    issued_time      timestamp with time zone,
    value            numeric,
    src              text,
    monitoring_date  date,
    issued_date      date,
    valid_date       date,
    CONSTRAINT {TABLE}_unique UNIQUE (monitoring_date, valid_date, src)
);
GRANT SELECT ON projects.{TABLE} TO dbreader;
"""


def _run(engine, sql, label):
    for stmt in [x.strip() for x in sql.split(";") if x.strip()]:
        print(f"[{label}] {stmt.splitlines()[0]}")
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except ProgrammingError as e:
            if "InsufficientPrivilege" in str(e) or "permission denied" in str(e):
                print(f"[{label}]   -> skipped (insufficient privilege): {e.orig}")
            else:
                raise


def schema_exists(engine):
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'projects'")
            ).scalar()
        )


def backfill(engine):
    df = stratus.load_parquet_from_blob(BLOB_PARQUET, stage="dev", container_name="projects")
    print(f"backfilling {len(df)} rows from dev blob {BLOB_PARQUET}")
    df.to_sql(
        TABLE,
        schema="projects",
        con=engine,
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=5000,
    )
    with engine.connect() as conn:
        n, mn, mx = conn.execute(
            text(f"SELECT count(*), min(monitoring_date), max(monitoring_date) FROM projects.{TABLE}")
        ).one()
    print(f"projects.{TABLE}: {n} rows, monitoring_date {mn} .. {mx}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dev", "prod"], required=True)
    ap.add_argument("--backfill", action="store_true", help="upsert the dev history from blob")
    args = ap.parse_args()
    engine = stratus.get_engine(args.mode, write=True)
    _run(engine, SCHEMA_SQL, "schema")
    if not schema_exists(engine):
        raise SystemExit(
            "projects schema does not exist and could not be created: a DB admin "
            "must run the CREATE SCHEMA / GRANT statements in SCHEMA_SQL first"
        )
    _run(engine, TABLE_SQL, "table")
    if args.backfill:
        backfill(engine)
    print(f"projects.{TABLE} ready in {args.mode}")
