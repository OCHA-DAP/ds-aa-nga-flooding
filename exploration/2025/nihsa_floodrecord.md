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

# NiHSA flood record
<!-- markdownlint-disable MD013 -->

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd

from src.datasources import nihsa, codab
from src.utils import blob
from src.constants import *
```

```python
adm2 = codab.load_codab_from_blob(admin_level=2)
```

```python
adm2
```

```python
import src.constants

blob_name = f"{src.constants.PROJECT_PREFIX}/raw/AA-nigeria_data/NiHSA/LGA_Flood_History 2013-2023.xlsx"
dfs = blob.load_excel_from_blob(blob_name, sheet_name=None)
```

```python
df_nihsa = (
    pd.concat(dfs.values(), keys=dfs.keys(), names=["Year", "Index"])
    .reset_index()
    .drop(columns="Index")
)
df_nihsa["LGA"] = df_nihsa["LGA"].fillna(df_nihsa["LGAs"])
df_nihsa["State"] = df_nihsa["State"].fillna(df_nihsa["STATE"])
df_nihsa = df_nihsa.drop(columns=["LGAs", "STATE"])
df_nihsa["Flooded"] = df_nihsa["Flooded"].astype(bool)
df_nihsa["Year"] = df_nihsa["Year"].astype(int)
df_nihsa = df_nihsa.dropna()
df_nihsa = df_nihsa[~df_nihsa["LGA"].isin(["Lake chad", "Lagos Lagoon"])]
df_nihsa["LGA"] = df_nihsa["LGA"].replace(
    {
        "Bukuru": "Buruku",
        "Koton-Karfe": "Kogi",
        "Girie": "Girei",
        "Teungo": "Toungo",
        "Borsari": "Bursari",
        "Barde": "Bade",
    }
)
```

```python
df_nihsa = df_nihsa[df_nihsa["State"].isin(["Adamawa", "Borno", "Yobe"])]
```

```python
df_nihsa
```

```python
import pandas as pd
from thefuzz import process

# Example dataframes: df_nihsa and adm2
# df_nihsa['LGA'] should match adm2['ADM2_EN']


def match_LGAs(df_nihsa, adm2, threshold=90):
    # Create an empty dictionary to store the matched values
    matches = {}

    # Iterate over each LGA in df_nihsa
    for lga in df_nihsa["LGA"]:
        # Use thefuzz to find the best match in the 'ADM2_EN' column of adm2
        result = process.extractOne(lga, adm2["ADM2_EN"])

        # Print the result to inspect its structure (for debugging)
        # print(f"Result for '{lga}': {result}")

        # Check if the result is a tuple or dictionary
        if isinstance(result, tuple):
            best_match, score, _ = result
        elif isinstance(result, dict):
            best_match = result.get("string")
            score = result.get("score", 0)
        else:
            best_match, score = None, 0

        # If the score is above the threshold, consider it a match
        if score >= threshold:
            matches[lga] = best_match
        else:
            matches[lga] = None  # No match found

    # Add a new column 'matched_ADM2_EN' to df_nihsa based on the matches
    df_nihsa["matched_ADM2_EN"] = df_nihsa["LGA"].map(matches)

    # Identify LGAs that did not get matched
    unmatched = df_nihsa[df_nihsa["matched_ADM2_EN"].isna()]

    # Provide a report of unmatched entries for manual assignment
    if not unmatched.empty:
        print("The following LGAs were not automatically matched:")
        print(unmatched[["LGA"]])
        print("\nPlease manually assign matches to these LGAs.")

    # Confirm all LGA entries got matched (if you want to raise an error if not)
    if unmatched.shape[0] > 0:
        raise ValueError(
            f"{unmatched.shape[0]} LGAs were not matched. Please check the unmatched list above."
        )

    return df_nihsa


# Run the function with your dataframes
df_nihsa = match_LGAs(df_nihsa, adm2)
```

```python
adm2[adm2["ADM1_PCODE"].isin(AOI_ADM1_PCODES)].iloc[:60]
```

```python
adm2[adm2["ADM1_PCODE"].isin(AOI_ADM1_PCODES)].iloc[60:]
```

```python
df_nihsa = df_nihsa.rename(columns={"matched_ADM2_EN": "ADM2_EN"})
```

```python
df_nihsa
```

```python
df_nihsa = df_nihsa.merge(adm2[["ADM2_EN", "ADM2_PCODE"]])
```

```python
df_nihsa
```

```python
import src.constants

blob_name = (
    f"{src.constants.PROJECT_PREFIX}/processed/nihsa/floodhistory_2013_2023.parquet"
)
```

```python
blob.upload_parquet_to_blob(blob_name, df_nihsa.drop(columns=["State", "LGA"]))
```

```python

```
