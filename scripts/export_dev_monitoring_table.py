"""Export the DEV monitoring table to a parquet in the DEV blob.

Half of the dev → prod carry-over (the other half is
scripts/init_prod_projects_schema.py --backfill, run on Databricks): the dev
DB is only reachable from a laptop on the right network, and the prod DB is
only writable from Databricks, but both can reach the dev blob.

    python scripts/export_dev_monitoring_table.py
"""

import ocha_stratus as stratus
import pandas as pd

TABLE = "ds_aa_nga_flooding_monitoring"
BLOB_PARQUET = "ds-aa-nga-flooding/migration/projects_ds_aa_nga_flooding_monitoring.parquet"

if __name__ == "__main__":
    df = pd.read_sql(f"SELECT * FROM projects.{TABLE}", stratus.get_engine("dev"))
    print(f"{len(df)} rows, monitoring_date {df.monitoring_date.min()} .. {df.monitoring_date.max()}")
    stratus.upload_parquet_to_blob(df, BLOB_PARQUET, stage="dev", container_name="projects")
    print(f"uploaded to dev blob projects/{BLOB_PARQUET}")
