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

# Benue riverine LGAs
FUFORE2 = "NG002002"
YOLASOUTH2 = "NG002021"
YOLANORTH2 = "NG002020"
GIREI2 = "NG002005"
DEMSA2 = "NG002001"
NUMAN2 = "NG002016"
LAMURDE2 = "NG002009"

BENUE_ADM2_PCODES = [
    FUFORE2,
    YOLASOUTH2,
    YOLANORTH2,
    GIREI2,
    DEMSA2,
    NUMAN2,
    LAMURDE2,
]


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

# --- 2025 framework thresholds (superseded by the 2026 framework below;
# kept for historical notebooks/analysis) ---
GLOFAS_THRESH = 3132
GOOGLE_THRESH = 1195

GLOFAS_WARNING_THRESH = 2800
GOOGLE_WARNING_THRESH = 1000

# --- 2026 endorsed framework (Adamawa riverine) ---
# Action trigger: fires when >= ACTION_MIN_GAUGES of the 10 selected Google
# GRRR gauges simultaneously exceed their individual 4-yr empirical Weibull
# RP threshold (wet-season Aug-Nov annual maxima, GRRR reanalysis 1998-2023)
# on the same valid day. Thresholds from
# exploration/2026/cerf/workflow/06_trigger_definition.ipynb.
ACTION_GAUGE_THRESHOLDS = {
    "hybas_1120842990": 1110.8,
    "hybas_1120843610": 1101.5,
    "hybas_1120845060": 1102.2,
    "hybas_1120849600": 1113.7,
    "hybas_1120848550": 1106.0,
    "hybas_1121970280": 1110.1,
    "hybas_1120842550": 1114.0,  # Wuroboki / Kangli (quality-verified)
    "hybas_1120840700": 1112.6,
    "hybas_1120840560": 1117.1,
    "hybas_1120840690": 142.7,  # small-tributary outlier, retained by design
}
ACTION_MIN_GAUGES = 6

# Readiness trigger (Option A): GloFAS ensemble-mean forecast at Wuroboki
# exceeds 3,132 m3/s at lead time <= 13 days. From
# exploration/2026/cerf/workflow/07_readiness_trigger.ipynb.
READINESS_GLOFAS_THRESH = 3132.0
READINESS_MAX_LEADTIME = 13  # days

# --- Listmonk email dispatch ---
# Lists are resolved by tag at runtime (never by hardcoded id). Created by
# pipelines/setup_nga_listmonk_lists.py.
LISTMONK_PROJECT_TAG = "ds-aa-nga-flooding"
LISTMONK_LISTS = {
    "info": {
        "name": "[AA framework] Nigeria flooding (informational)",
        "tag": "nga:info",
    },
    "trigger": {
        "name": "[AA framework] Nigeria flooding (trigger)",
        "tag": "nga:trigger",
    },
    # Test recipients. Created by the setup script but NOT populated from the
    # distribution list; add dev/test subscribers manually. When STAGE != prod
    # the dispatch routes ALL sends here instead of info/trigger, so real
    # sends can be exercised without reaching the real audience.
    "test": {
        "name": "[TEST] Nigeria flooding",
        "tag": "nga:test",
        "extra_tags": ["TEST"],
    },
}
