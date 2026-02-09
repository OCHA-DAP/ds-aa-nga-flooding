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

# Targerted LGAs

Produce map to show targeted LGAs:

- Blue: riverine flooding (CERF and NHF)
- Orange: flash flooding (NHF only)

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import codab
from src.constants import *
```

```python
adm1 = codab.load_codab_from_blob(admin_level=1, aoi_only=True)
adm2 = codab.load_codab_from_blob(admin_level=2, aoi_only=True)
```

```python
fig, ax = plt.subplots(dpi=200)

adm2[adm2["ADM2_PCODE"].isin(NHF_FLASH_LGAS)].plot(ax=ax, color="darkorange")
adm2[adm2["ADM2_PCODE"].isin(BENUE_ADM2_PCODES)].plot(ax=ax, color="royalblue")

adm2.boundary.plot(ax=ax, linewidth=0.2, color="k")
adm1.boundary.plot(ax=ax, linewidth=1, color="k")
ax.axis("off")
```
