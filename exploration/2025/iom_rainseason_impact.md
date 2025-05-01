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

# IOM rain season flood data

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from thefuzz import process

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
adm2["ADM2_EN_lower"] = adm2["ADM2_EN"].str.lower()
```

```python
import src.constants

blob_name = (
    f"{src.constants.PROJECT_PREFIX}/raw/impact/iom/Rain season historical data.xlsx"
)
```

```python
df = blob.load_excel_from_blob(blob_name)
```

```python
df["LGA_lower"] = df["LGA"].str.lower()
```

```python
"abadam" in adm2["ADM2_EN"].str.lower().to_list()
```

```python
matches_dict = {
    "maiduguri m. c.": "maiduguri",
    "kala balge": "kala/balge",
    "tarmuwa": "tarmua",
}
```

```python
for lga in df["LGA_lower"].unique():
    if not (lga in adm2["ADM2_EN_lower"].to_list() or lga in matches_dict):
        matches = process.extract(
            lga, adm2["ADM2_EN_lower"].to_list(), limit=5
        )
        print(lga)
        print(matches)
        break
```

```python
df["ADM2_EN_lower"] = df["LGA"].str.lower().replace(matches_dict)
```

```python
df_matched = df.merge(adm2[["ADM2_EN_lower", "ADM2_PCODE", "ADM2_EN"]])
```

```python
df_matched[df_matched["ADM2_PCODE"].isnull()]
```

```python
df_matched.dtypes
```

```python
df_matched.columns
```

```python
df_matched["INCIDENT DATE"].dt.dayofyear.hist()
```

```python
df_matched["INCIDENT DATE"].min()
```

```python
df_matched["INCIDENT DATE"].max()
```

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/processed/iom/rainseason_2021_2024.parquet"
```

```python
blob.upload_parquet_to_blob(blob_name, df_matched)
```
