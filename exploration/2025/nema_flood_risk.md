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

# NEMA flood risk
<!-- markdownlint-disable MD013 -->

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import codab
from src.utils import blob
from src.constants import *
```

```python
adm2 = codab.load_codab_from_blob(aoi_only=True, admin_level=2)
```

```python
adm2
```

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NEMA/Flood Risk Excel Data 2.xlsx"
```

```python
df = blob.load_excel_from_blob(blob_name)
```

```python
df = df[df["ADM1_PCODE"].isin(AOI_ADM1_PCODES)]
```

```python
df["Riverine Flood Risk"].value_counts()
```

```python
df["Flash Flood Risk"].value_counts()
```

```python
df["Coastal Flood Risk"].value_counts()
```

```python
df.merge(adm2)
```
