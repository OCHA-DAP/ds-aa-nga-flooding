"""Databricks entrypoint: download whole-Nigeria GloFAS reforecast.

Downloads the cems-glofas-reforecast dataset for the full Nigeria bounding
box (NGA_BBOX) and saves one GRIB2 file per (year, leadtime chunk) to blob
storage under:

  ds-aa-nga-flooding/raw/glofas/reforecast_country/
    glofas_raw_reforecast_country_{product_type}_{year}_lt{lt_start}-{lt_end}.grib

The job is idempotent: it skips blobs that already exist.

Usage (local):
    python pipelines/download_glofas_reforecast_country.py

Deploy and run on Databricks:
    databricks bundle validate -t dev -p DEFAULT
    databricks bundle deploy   -t dev -p DEFAULT
    databricks bundle run download_glofas_reforecast_country -t dev -p DEFAULT

Override parameters at run time (example — control product):
    databricks bundle run download_glofas_reforecast_country \\
        -t dev -p DEFAULT \\
        --python-named-params "product_type=control"


Size estimates (whole-country bbox, 0.05° grid, ~200×240 pixels):
  Rainy season + 16-day leadtime  →  ~58 GB   (88 files: 22 yr × 4 chunks)
  All months   + 46-day leadtime  →  ~430 GB  (528 files: 22 yr × 12 chunks)

Runtime estimate: ~30–60 min per CDS request → 2–4 days for rainy/16d scope.
"""

import argparse

from src.datasources.glofas import download_glofas_reforecast_country


def main() -> None:
    p = argparse.ArgumentParser(
        description="Download country-wide GloFAS reforecast to blob."
    )
    p.add_argument(
        "--product-type",
        default="ensemble",
        choices=["ensemble", "control"],
        help="CDS product type (default: ensemble)",
    )
    p.add_argument(
        "--start-year",
        type=int,
        default=2003,
        help="First historical year to download (default: 2003)",
    )
    p.add_argument(
        "--end-year",
        type=int,
        default=2024,
        help="Last historical year to download, inclusive (default: 2024)",
    )
    p.add_argument(
        "--max-leadtime-days",
        type=int,
        default=16,
        help="Maximum leadtime in days (default: 16; max available: 46)",
    )
    p.add_argument(
        "--max-leadtime-chunk",
        type=int,
        default=4,
        help="Leadtime days per CDS request (default: 4)",
    )
    p.add_argument(
        "--rainy-season-only",
        type=lambda x: x.lower() == "true",
        default=True,
        metavar="true|false",
        help="Restrict to rainy season months Jun–Dec (default: true)",
    )
    args = p.parse_args()

    print(
        f"Starting country-wide GloFAS reforecast download: "
        f"product_type={args.product_type}, "
        f"years={args.start_year}–{args.end_year}, "
        f"max_leadtime_days={args.max_leadtime_days}, "
        f"rainy_season_only={args.rainy_season_only}"
    )

    download_glofas_reforecast_country(
        product_type=args.product_type,
        years=range(args.start_year, args.end_year + 1),
        max_leadtime_days=args.max_leadtime_days,
        max_leadtime_chunk=args.max_leadtime_chunk,
        rainy_season_only=args.rainy_season_only,
    )

    print("Done.")


if __name__ == "__main__":
    main()
