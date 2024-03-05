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

# Floodscan

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.datasources import floodscan, codab, worldpop
```

```python
adm2 = codab.load_codab(admin_level=2)
```

```python
adm2.plot()
```

```python
aoi = adm2[adm2["ADM1_EN"].isin(["Borno", "Adamawa", "Yobe"])]
```

```python
aoi["ADM2_PCODE"].unique()
```

```python
pop = worldpop.load_raw_worldpop()
```

```python
pop.plot()
```

```python
floodscan.clip_nga_from_glb()
```

```python
da = floodscan.load_raw_nga_floodscan()
```

```python
da.mean(dim=["lat", "lon"]).plot()
```

```python
da
```

```python
da_year = da.groupby("time.year").max()
da_year_mask = da_year.where(da_year >= 0.05)
da_year_mask = da_year_mask.rio.write_crs(4326)
da_year_mask = da_year_mask.transpose("year", "lat", "lon")
```

```python
da_year_mask
```

```python
da_year_mask_resample = da_year_mask.rio.reproject_match(pop)
da_year_mask_resample = da_year_mask_resample.where(da_year_mask_resample <= 1)
```

```python
exposure = da_year_mask_resample * pop.isel(band=0)
```

```python
exposure.isel(year=10).plot()
```

```python
dfs = []

for _, row in adm2.iterrows():
    da_clip = exposure.rio.clip([row.geometry])
    dff = (
        da_clip.sum(dim=["x", "y"])
        .to_dataframe(name="total_exposed")["total_exposed"]
        .astype(int)
        .reset_index()
    )
    dff["ADM2_PCODE"] = row["ADM2_PCODE"]
    dfs.append(dff)

df = pd.concat(dfs, ignore_index=True)
```

```python
df
```

```python
filename = "nga_adm2_count_flood_exposed.csv"
df.to_csv(floodscan.PROC_FS_DIR / filename, index=False)
```
