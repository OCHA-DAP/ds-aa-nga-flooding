import geopandas as gpd
import ocha_stratus as stratus
from dotenv import load_dotenv

load_dotenv()

GDB_PATH = "exploration/2026/data/HydroRIVERS_v10_af.gdb"
OUT_PATH = "exploration/2026/cerf/shapefiles/hydrorivers_bay.shp"
BAY_STATES = ["Borno", "Adamawa", "Yobe"]

# BAY states boundary
gdf_adm1 = stratus.codab.load_codab_from_blob("NGA", admin_level=1)
gdf_bay = gdf_adm1[gdf_adm1["ADM1_EN"].isin(BAY_STATES)]
bay_union = gdf_bay.union_all()

# Load and clip HydroRIVERS
print("Loading HydroRIVERS (this may take a moment)...")
gdf_rivers = gpd.read_file(GDB_PATH, layer="HydroRIVERS_v10_af")
print(f"Loaded {len(gdf_rivers):,} river segments")  # noqa: E231

gdf_clipped = gpd.clip(gdf_rivers, bay_union)
print(
    f"Clipped to {len(gdf_clipped):,} segments within BAY states"  # noqa: E231
)

gdf_clipped.to_file(OUT_PATH)
print(f"Saved to {OUT_PATH}")
