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

# Floodscan Benue

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import floodscan, codab, hydrosheds
from src.constants import *
```

```python
benue = hydrosheds.load_benue_aoi()
```

```python
adm2 = codab.load_codab(admin_level=2)
adm2_a = adm2[adm2["ADM1_PCODE"] == ADAMAWA]
adm2_a_pcodes = adm2_a["ADM2_PCODE"].unique()
```

```python
adm2_a.plot()
```

```python
df = floodscan.load_adm2_daily_rasterstats()
df = df[df["ADM2_PCODE"].isin(adm2_a_pcodes)]
```

```python
year_mean = df.groupby(df["time"].dt.year)["SFED_AREA"].mean().reset_index()
```

```python
year_mean
```

```python
df_max_adm2 = df.groupby("ADM2_PCODE")["SFED_AREA"].max().reset_index()
```

```python
df_max_adm2.sort_values("SFED_AREA", ascending=False)
```

```python
fig, ax = plt.subplots(dpi=300)
adm2_a.merge(df_max_adm2).plot(column="SFED_AREA", cmap="Blues", ax=ax)
adm2_a.boundary.plot(linewidth=0.1, color="k", ax=ax)
benue.plot(ax=ax, color="red", linewidth=1)

ax.axis("off")
```
