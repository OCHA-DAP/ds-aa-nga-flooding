"""Databricks entrypoint: process country-wide GloFAS reforecast into per-station parquets.

Reads the country-wide GRIBs downloaded by download_glofas_reforecast_country,
extracts one pixel per station defined in GF_STATIONS, and saves processed
parquets to blob under:

  ds-aa-nga-flooding/processed/glofas/
    glofas_reforecast_{station_name}_{product_type}.parquet

Each GRIB is downloaded once, all station pixels extracted, then deleted —
peak local disk usage is ~1 GRIB (~600 MB) at a time.

Usage (local):
    python pipelines/process_glofas_reforecast_country.py

Deploy and run on Databricks:
    databricks bundle deploy   -t dev -p DEFAULT --var git_branch=<branch>
    databricks bundle run process_glofas_reforecast_country -t dev -p DEFAULT \\
        --var git_branch=<branch>
"""

import argparse

from src.datasources.glofas import process_glofas_reforecast_country


def main() -> None:
    p = argparse.ArgumentParser(
        description="Process country-wide GloFAS reforecast GRIBs into per-station parquets."
    )
    p.add_argument(
        "--product-type",
        default="ensemble",
        choices=["ensemble", "control"],
    )
    p.add_argument("--start-year", type=int, default=2003)
    p.add_argument("--end-year", type=int, default=2023)
    p.add_argument("--max-leadtime-days", type=int, default=16)
    p.add_argument("--max-leadtime-chunk", type=int, default=4)
    p.add_argument(
        "--rainy-season-only",
        type=lambda x: x.lower() == "true",
        default=True,
        metavar="true|false",
    )
    p.add_argument("--prod-dev", default="dev", choices=["dev", "prod"])
    args = p.parse_args()

    print(
        f"Processing GloFAS reforecast: product_type={args.product_type}, "
        f"years={args.start_year}-{args.end_year}, "
        f"rainy_season_only={args.rainy_season_only}, "
        f"prod_dev={args.prod_dev}"
    )

    process_glofas_reforecast_country(
        product_type=args.product_type,
        years=range(args.start_year, args.end_year + 1),
        max_leadtime_days=args.max_leadtime_days,
        max_leadtime_chunk=args.max_leadtime_chunk,
        rainy_season_only=args.rainy_season_only,
        prod_dev=args.prod_dev,
    )

    print("Done.")


if __name__ == "__main__":
    main()
