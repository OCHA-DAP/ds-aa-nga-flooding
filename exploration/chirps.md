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

# CHIRPS

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import xarray as xr
import cftime
import pandas as pd
import rioxarray as rxr
import matplotlib.pyplot as plt
from tqdm.notebook import tqdm

from src.datasources import codab, chirps
```

```python
# run only this cell only for downloading

import pandas as pd
from tqdm.notebook import tqdm

from src.datasources import codab, chirps

# codab.download_codab()

adm = codab.load_codab(admin_level=2, aoi_only=True)

dates = pd.date_range(start="1998-01-01", end="2023-12-31", freq="D")

for date in tqdm(dates):
    chirps.download_chirps_daily(date, adm.total_bounds)
```

```python
adm = codab.load_codab(admin_level=2, aoi_only=True)
```

```python
adm.plot()
```

```python
chirps.process_chirps_daily()
```

```python
chirps.calculate_raster_stats()
```

```python
ds = chirps.load_chirps_daily()
```

```python
fig, ax = plt.subplots()
ds["prcp"].isel(T=10).plot(ax=ax)
adm.boundary.plot(ax=ax)
```

```python
df = chirps.load_raster_stats()
```

```python

```
