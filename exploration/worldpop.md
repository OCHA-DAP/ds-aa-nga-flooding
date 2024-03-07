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

# WorldPop

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import worldpop, codab
from src.constants import AOI_ADM1_PCODES
```

```python
adm2 = codab.load_codab(admin_level=2)
adm2_aoi = adm2[adm2["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
```

```python
da = worldpop.load_raw_worldpop()
da_aoi = da.rio.clip(adm2_aoi.geometry, all_touched=True)
da_aoi = da_aoi.where(da_aoi > 0)
```

```python
fig, ax = plt.subplots(figsize=(10, 10))
da_aoi.plot(ax=ax, cmap="Greys", vmax=500)
adm2_aoi.boundary.plot(ax=ax, linewidth=0.2, color="k")
ax.axis("off")
ax.set_title("Population, 2020")
```

```python
da.rio.crs
```
