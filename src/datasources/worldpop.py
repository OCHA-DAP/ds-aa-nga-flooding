import os
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import rioxarray as rxr
import xarray as xr
from pystac_client import Client

from src.datasources import codab

WORLDPOP_STAC_URL = "https://api.stac.worldpop.org"

AA_DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW", "."))
RAW_WP_PATH = (
    AA_DATA_DIR
    / "public"
    / "raw"
    / "nga"
    / "worldpop"
    / "nga_ppp_2020_1km_Aggregated_UNadj.tif"
)
PROC_WP_DIR = AA_DATA_DIR / "public" / "processed" / "nga" / "worldpop"


def load_raw_worldpop():
    da = rxr.open_rasterio(RAW_WP_PATH)
    return da.where(da != da.attrs["_FillValue"])


def aggregate_worldpop_to_adm2():
    pop = load_raw_worldpop()
    adm2 = codab.load_codab(admin_level=2)
    dicts = []
    for _, row in adm2.iterrows():
        da_clip = pop.rio.clip([row.geometry])
        da_clip = da_clip.where(da_clip > 0)
        dicts.append(
            {
                "total_pop": da_clip.sum().values,
                "ADM2_PCODE": row["ADM2_PCODE"],
            }
        )
    df_pop = pd.DataFrame(dicts)
    filename = "nga_adm2_2020_1km_Aggregated_UNadj.csv"
    df_pop.to_csv(PROC_WP_DIR / filename, index=False)


def load_adm2_worldpop():
    filename = "nga_adm2_2020_1km_Aggregated_UNadj.csv"
    return pd.read_csv(PROC_WP_DIR / filename)


def load_worldpop_from_stac(
    iso3: str,
    year: int = 2025,
    resolution: str = "100m",
    product: str = "pop",
    item_id: str | None = None,  # Allow exact item ID
    cache_dir: Path | str | None = None,
) -> xr.DataArray:
    """
    Get WorldPop population data for a country.

    Parameters
    ----------
    iso3 : str
        ISO3 country code (e.g., "CUB", "HTI", "JAM")
    year : int
        Year (2015-2030 available)
    resolution : str
        "100m" or "1km"
    product : str
        "pop" for total population, "agesex" for age/sex breakdown
    item_id : str, optional
        Exact STAC item ID. If provided, other filters are ignored.
    cache_dir : Path, optional
        Directory to cache downloaded files.

    Returns
    -------
    xr.DataArray
    """
    client = Client.open(WORLDPOP_STAC_URL)

    search = client.search(
        filter={
            "op": "=",
            "args": [{"property": "Alpha-3 code"}, iso3.upper()],
        }
    )

    if item_id:
        # Exact match
        matching = [item for item in search.items() if item.id == item_id]
    else:
        prefix = f"{iso3.lower()}_{product}_{year}"
        matching = [
            item
            for item in search.items()
            if item.id.startswith(prefix) and f"_{resolution}_" in item.id
        ]

    if not matching:
        raise ValueError(
            f"No data found for {iso3}, {product}, {year}, {resolution}"
        )

    if len(matching) > 1:
        ids = [item.id for item in matching]
        raise ValueError(
            f"Multiple items found: {ids}. "
            f"Pass item_id='...' to select one explicitly."
        )

    item = matching[0]
    cog_url = item.assets["data"].href

    # Download the file (server doesn't support range requests)
    if cache_dir:
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        local_path = cache_dir / f"{item.id}.tif"
        if not local_path.exists():
            print(f"Downloading {item.id}...")
            urllib.request.urlretrieve(cog_url, local_path)
    else:
        # Use temp directory
        tmp_dir = TemporaryDirectory()
        local_path = Path(tmp_dir.name) / f"{item.id}.tif"
        print(f"Downloading {item.id}...")
        urllib.request.urlretrieve(cog_url, local_path)

    da = xr.open_dataarray(local_path, engine="rasterio")
    if da.ndim == 3 and da.shape[0] == 1:
        da = da.squeeze("band", drop=True)

    da.attrs.update(
        {
            "iso3": iso3.upper(),
            "year": year,
            "resolution": resolution,
            "product": product,
            "item_id": item.id,
            "source_url": cog_url,
        }
    )
    return da
