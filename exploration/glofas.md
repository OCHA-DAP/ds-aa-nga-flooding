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

# GloFAS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import datetime

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.datasources import glofas
from src.constants import *
```

```python
# glofas.process_reanalysis()
```

```python
# glofas.download_reforecast()
```

```python
# glofas.process_reforecast()
```

```python
ref = glofas.load_reforecast()
rea = glofas.load_reanalysis()
rea = rea[rea["time"].dt.year.isin(ref["time"].dt.year.unique())]
```

```python
rp_f = 3
rp_a = 5

dfs = []

for lt in ref["leadtime"].unique():
    dff = ref[ref["leadtime"] <= lt]
    df_in = dff.loc[dff.groupby(dff["time"].dt.year)["dis24"].idxmax()]
    df_in["lt_max"] = lt
    thresh = df_in["dis24"].quantile(1 - 1 / rp_f)
    df_in["trigger"] = df_in["dis24"] > thresh
    print(lt, thresh)
    dfs.append(df_in)

ref_peaks = pd.concat(dfs, ignore_index=True)
ref_peaks["year"] = ref_peaks["time"].dt.year
```

```python
rea_peaks = rea.loc[rea.groupby(rea["time"].dt.year)["dis24"].idxmax()]
q = rea_peaks["dis24"].quantile(1 - 1 / rp_a)
rea_peaks["trigger"] = rea_peaks["dis24"] > q
rea_peaks["year"] = rea_peaks["time"].dt.year
rea_peaks["cerf"] = rea_peaks["year"].isin(CERF_YEARS)
```

```python
compare = rea_peaks.merge(ref_peaks, on="year", suffixes=["_a", "_f"])
```

```python
for indicator in ["cerf", "trigger_a"]:
    compare[f"TP_{indicator}"] = compare[indicator] & compare["trigger_f"]
    compare[f"FP_{indicator}"] = ~compare[indicator] & compare["trigger_f"]
    compare[f"TN_{indicator}"] = ~compare[indicator] & ~compare["trigger_f"]
    compare[f"FN_{indicator}"] = compare[indicator] & ~compare["trigger_f"]
```

```python
dicts = []
for lt, group in compare.groupby("lt_max"):
    TPR = group["TP_trigger_a"].sum() / group["trigger_a"].sum()
    PPV = group["TP_trigger_a"].sum() / group["trigger_f"].sum()
    TPR_C = group["TP_cerf"].sum() / group["cerf"].sum()
    PPV_C = group["TP_cerf"].sum() / group["trigger_f"].sum()
    dicts.append(
        {"TPR": TPR, "PPV": PPV, "TPR_C": TPR_C, "PPV_C": PPV_C, "lt_max": lt}
    )

metrics = pd.DataFrame(dicts)
```

```python
metrics
```

```python
compare[compare["lt_max"] == 1]
```

```python
rea[rea["time"].dt.year == 2014].plot(x="time", y="dis24")
```

```python
ref[(ref["time"].dt.year == 2014) & (ref["leadtime"] == 1)].plot(
    x="time", y="dis24"
)
```
