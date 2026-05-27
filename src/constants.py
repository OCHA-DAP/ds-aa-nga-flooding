CERF_YEARS = [2013, 2018, 2022]
ADAMAWA = "NG002"
WUROBOKI_LAT = 9.375
WUROBOKI_LON = 12.78
# new values for 2025 reanalysis
WUROBOKI_LAT_NEW = 9.375
WUROBOKI_LON_NEW = 12.775

WUROBOKI_2YRPR = 2200
WUROBOKI_3YRPR = 2669
# this is a visual guess from the GloFAS plot
WUROBOKI_5YRPR = 6800
BORNO = "NG008"
AOI_ADM1_PCODES = ["NG008", "NG036", "NG002"]
AOI_ADM2_PCODES = [
    "NG008001",
    "NG008002",
    "NG036001",
    "NG008003",
    "NG008004",
    "NG008005",
    "NG036002",
    "NG008006",
    "NG036003",
    "NG008007",
    "NG002001",
    "NG008008",
    "NG036004",
    "NG002002",
    "NG036005",
    "NG002003",
    "NG036006",
    "NG002005",
    "NG002004",
    "NG008009",
    "NG036007",
    "NG036008",
    "NG002006",
    "NG008010",
    "NG008011",
    "NG008012",
    "NG002007",
    "NG002008",
    "NG036009",
    "NG008013",
    "NG008014",
    "NG008015",
    "NG036010",
    "NG008016",
    "NG008017",
    "NG008018",
    "NG002009",
    "NG036011",
    "NG002010",
    "NG008019",
    "NG008020",
    "NG008021",
    "NG002011",
    "NG008022",
    "NG002012",
    "NG002013",
    "NG008023",
    "NG008024",
    "NG002014",
    "NG002015",
    "NG036012",
    "NG008025",
    "NG008026",
    "NG036013",
    "NG002016",
    "NG036014",
    "NG008027",
    "NG002017",
    "NG002018",
    "NG036015",
    "NG002019",
    "NG002020",
    "NG002021",
    "NG036016",
    "NG036017",
]

JAKUSKO = "NG036009"
JERE = "NG008013"
NGALA = "NG008025"
DIKWA = "NG008008"

MONGUNO = "NG008024"
MARTE = "NG008022"
MAFA = "NG008019"

EXP_HRP_BORNO_ADM2_PCODES = [JERE, NGALA, DIKWA]
EXP_HRP_ADM2_PCODES = [JERE, NGALA, DIKWA, JAKUSKO]

BAMA = "NG008003"
NUMAN = "NG002016"

PRIORITYONE_ADM2_PCODES = [BAMA, NUMAN]
PRIORITYTWO_ADM2_PCODES = [DIKWA, NGALA]

ALL_PRIORITY_ADM2_PCODES = PRIORITYONE_ADM2_PCODES + PRIORITYTWO_ADM2_PCODES

BENUE = "NG007"

# Benue state — riverine LGAs along the Benue river
AGATU2 = "NG007002"
GUMA2 = "NG007006"
GWER_WEST2 = "NG007008"
LOGO2 = "NG007012"
MAKURDI2 = "NG007013"

BENUE_STATE_ADM2_PCODES = [
    AGATU2,
    GUMA2,
    GWER_WEST2,
    LOGO2,
    MAKURDI2,
]

# Adamawa — riverine LGAs along the Benue river
FUFORE2 = "NG002002"
YOLASOUTH2 = "NG002021"
YOLANORTH2 = "NG002020"
GIREI2 = "NG002005"
DEMSA2 = "NG002001"
NUMAN2 = "NG002016"
LAMURDE2 = "NG002009"

ADAMAWA_ADM2_PCODES = [
    FUFORE2,
    YOLASOUTH2,
    YOLANORTH2,
    GIREI2,
    DEMSA2,
    NUMAN2,
    LAMURDE2,
]
BENUE_ADM2_PCODES = ADAMAWA_ADM2_PCODES  # backwards-compatible alias


# NHF flash LGAs
BAMA2 = "NG008003"
BADE2 = "NG036001"
KARASUWA2 = "NG036010"
MADAGALI2 = "NG002010"
MAIDUGURI2 = "NG008021"
NGALA2 = "NG008025"

NHF_FLASH_LGAS = [
    BAMA2,
    BADE2,
    KARASUWA2,
    MADAGALI2,
    MAIDUGURI2,
    NGALA2,
]

PROJECT_PREFIX = "ds-aa-nga-flooding"

GLOFAS_THRESH = 3132
GOOGLE_THRESH = 1195

GLOFAS_WARNING_THRESH = 2800
GOOGLE_WARNING_THRESH = 1000

# Google Flood Forecasting HYBAS gauge IDs
WUROBOKI_HYBAS = "hybas_1120842550"
MAKURDI_HYBAS = "hybas_1120911340"

# Per-state config for riverine flooding analysis notebooks
STATE_CONFIG = {
    "Benue": {
        "analysis_start_year": 1998,
        "analysis_end_year": 2023,
        "adm1_col": "ADM1_PCODE",
        "adm1_val": BENUE,
        "lga_pcodes": BENUE_STATE_ADM2_PCODES,
        "river_x_min": 7.5,
        "glofas_station": "makurdi",
        "station_label": "Makurdi GloFAS Station",
        "google_gauge": MAKURDI_HYBAS,
        "floodscan_blob": "ds-aa-nga-flooding/processed/floodscan/fs_benue_state_pixels_1998_2025.parquet",  # noqa
        "glofas_thresh": None,  # TBD via reforecast analysis
        "google_thresh": None,  # TBD via reforecast analysis
        "glofas_leadtime_action": None,
        "google_leadtime_action": None,
        "glofas_leadtime_readiness": None,
        "google_leadtime_readiness": None,
        "glofas_reforecast_blob": "ds-aa-nga-flooding/processed/glofas/glofas_reforecast_makurdi_ensemble.parquet",  # noqa
    },
    "Adamawa": {
        "analysis_start_year": 1998,
        "analysis_end_year": 2023,
        "adm1_col": "ADM1_PCODE",
        "adm1_val": ADAMAWA,
        "lga_pcodes": ADAMAWA_ADM2_PCODES,
        "river_x_min": 11.0,
        "glofas_station": "wuroboki",
        "station_label": "Wuroboki GloFAS Station",
        "google_gauge": WUROBOKI_HYBAS,
        "floodscan_blob": "ds-aa-nga-flooding/processed/floodscan/fs_adamawa_pixels_1998_2025.parquet",  # noqa
        "glofas_thresh": GLOFAS_THRESH,
        "google_thresh": GOOGLE_THRESH,
        "glofas_leadtime_action": 5,
        "google_leadtime_action": 7,
        "glofas_leadtime_readiness": None,
        "google_leadtime_readiness": None,
        "glofas_reforecast_blob": "ds-aa-nga-flooding/processed/glofas/wuroboki_glofas_reforecast_ens.parquet",  # noqa
    },
}
