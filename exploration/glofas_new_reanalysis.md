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

# GloFAS new reanalysis

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import xarray as xr

from src.datasources import nihsa, glofas
from src.utils import blob
```

```python
station_name = "wuroboki"
```

```python
glofas.download_glofas_reanalysis_to_blob(station_name=station_name)
```

```python
glofas.process_glofas_reanalysis(station_name=station_name)
```

```python
df_ra = glofas.load_glofas_reanalysis(station_name=station_name)
```

```python
df_ra
```

```python
df_nh = nihsa.load_wuroboki()
```

```python
df_nh
```
