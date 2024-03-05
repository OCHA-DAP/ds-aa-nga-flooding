---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-aa-nga-flooding
    language: python
    name: ds-aa-nga-flooding
---

# CODAB plot

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
```

```python
DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
GAUL_DIR = DATA_DIR / "public" / "raw" / "glb" / "asap" / "reference_data"
GAUL_ADM1_PATH = GAUL_DIR / "gaul1_asap_v04/gaul1_asap.shp"
GAUL_ADM0_PATH = GAUL_DIR / "gaul0_asap_v04/gaul0_asap.shp"
SAVE_DIR = DATA_DIR / "public" / "processed" / "nga" / "codab"
```

```python
adm1 = gpd.read_file(GAUL_ADM1_PATH)
adm0 = gpd.read_file(GAUL_ADM0_PATH)
```

```python
adm1
```

```python
asap0_id = 155
```

```python
asap1_ids = [1354, 1466, 1491]
```

```python
adm1_nga = adm1[adm1["asap0_id"] == asap0_id]
adm1_bay = adm1[adm1["asap1_id"].isin(asap1_ids)]
```

```python
lon_min, lon_max = -30, 100
lat_min, lat_max = -40, 40
scale_factor = 0.1

fig, ax = plt.subplots(
    figsize=(
        (lon_max - lon_min) * scale_factor,
        (lat_max - lat_min) * scale_factor,
    )
)

adm0.boundary.plot(ax=ax, linewidth=0.1, color="grey")
adm1_nga.boundary.plot(ax=ax, linewidth=0.3, color="k")
adm1_bay.boundary.plot(ax=ax, linewidth=0.8, color="red")
ax.set_xlim([lon_min, lon_max])
ax.set_ylim([lat_min, lat_max])
ax.axis("off")
filename = "bay_highlight.png"
plt.savefig(
    SAVE_DIR / filename, transparent=True, dpi=300, bbox_inches="tight"
)
plt.show()
```

```python

```

```python

```
