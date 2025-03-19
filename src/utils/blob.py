import io
import os
import shutil
import tempfile
import zipfile
from typing import Literal

import geopandas as gpd
import pandas as pd
from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

load_dotenv(override=True)

PROD_BLOB_SAS = os.getenv("PROD_BLOB_SAS")
PROD_BLOB_BASE_URL = "https://imb0chd0prod.blob.core.windows.net/"
PROD_BLOB_AA_BASE_URL = PROD_BLOB_BASE_URL + "aa-data"
PROD_BLOB_AA_URL = PROD_BLOB_AA_BASE_URL + "?" + PROD_BLOB_SAS

DEV_BLOB_SAS = os.getenv("DS_AZ_BLOB_DEV_SAS_WRITE")
DEV_BLOB_BASE_URL = "https://imb0chd0dev.blob.core.windows.net/"
DEV_BLOB_PROJ_BASE_URL = DEV_BLOB_BASE_URL + "projects"
DEV_BLOB_PROJ_URL = DEV_BLOB_PROJ_BASE_URL + "?" + DEV_BLOB_SAS

PROJECT_PREFIX = "ds-aa-nga-flooding"


prod_container_client = ContainerClient.from_container_url(PROD_BLOB_AA_URL)
dev_container_client = ContainerClient.from_container_url(DEV_BLOB_PROJ_URL)


def load_csv_from_blob(
    blob_name, prod_dev: Literal["prod", "dev"] = "dev", **kwargs
):
    blob_data = load_blob_data(blob_name, prod_dev=prod_dev)
    return pd.read_csv(io.BytesIO(blob_data), **kwargs)


def load_excel_from_blob(
    blob_name, prod_dev: Literal["prod", "dev"] = "dev", **kwargs
):
    blob_data = load_blob_data(blob_name, prod_dev=prod_dev)
    return pd.read_excel(io.BytesIO(blob_data), **kwargs)


def load_parquet_from_blob(
    blob_name, prod_dev: Literal["prod", "dev"] = "dev"
):
    blob_data = load_blob_data(blob_name, prod_dev=prod_dev)
    return pd.read_parquet(io.BytesIO(blob_data))


def upload_gdf_to_blob(
    gdf, blob_name, prod_dev: Literal["prod", "dev"] = "dev"
):
    with tempfile.TemporaryDirectory() as temp_dir:
        # File paths for shapefile components within the temp directory
        shp_base_path = os.path.join(temp_dir, "data")

        gdf.to_file(shp_base_path, driver="ESRI Shapefile")

        zip_file_path = os.path.join(temp_dir, "data")

        shutil.make_archive(
            base_name=zip_file_path, format="zip", root_dir=temp_dir
        )

        # Define the full path to the zip file
        full_zip_path = f"{zip_file_path}.zip"

        # Upload the buffer content as a blob
        with open(full_zip_path, "rb") as data:
            upload_blob_data(blob_name, data, prod_dev=prod_dev)


def load_gdf_from_blob(
    blob_name, shapefile: str = None, prod_dev: Literal["prod", "dev"] = "dev"
):
    blob_data = load_blob_data(blob_name, prod_dev=prod_dev)
    with zipfile.ZipFile(io.BytesIO(blob_data), "r") as zip_ref:
        zip_ref.extractall("temp")
        if shapefile is None:
            shapefile = [f for f in zip_ref.namelist() if f.endswith(".shp")][
                0
            ]
        gdf = gpd.read_file(f"temp/{shapefile}")
    return gdf


def load_blob_data(blob_name, prod_dev: Literal["prod", "dev"] = "dev"):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data


def upload_blob_data(
    blob_name, data, prod_dev: Literal["prod", "dev"] = "dev"
):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True)


def list_container_blobs(
    name_starts_with=None, prod_dev: Literal["prod", "dev"] = "dev"
):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    return [
        blob.name
        for blob in container_client.list_blobs(
            name_starts_with=name_starts_with
        )
    ]


def upload_parquet_to_blob(
    blob_name,
    df,
    prod_dev: Literal["prod", "dev"] = "dev",
    **kwargs,
):
    upload_blob_data(
        blob_name,
        df.to_parquet(**kwargs, index=False),
        prod_dev=prod_dev,
    )


def check_blob_exists(blob_name, prod_dev: Literal["prod", "dev"] = "dev"):
    if prod_dev == "dev":
        container_client = dev_container_client
    else:
        container_client = prod_container_client
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()
