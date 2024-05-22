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

# Benue

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import geopandas as gpd

from src.datasources import hydrosheds, codab
from src.constants import *
```

```python
adm2 = codab.load_codab(admin_level=2)
adm2_a = adm2[adm2["ADM1_PCODE"] == ADAMAWA]
```

```python
hydrosheds.process_benue_aoi()
```

```python
benue = hydrosheds.load_benue_aoi()
```

```python
ax = adm2_a.plot(alpha=0.2)
benue.plot(ax=ax)
```
