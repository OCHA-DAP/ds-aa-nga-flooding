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

# Backup DB test

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os

import pandas as pd
from sqlalchemy import create_engine
```

```python
AZURE_DB_BASE_URL = "postgresql+psycopg2://{uid}:{pw}@{host}/postgres"


def get_backup_db(write: bool = False):
    if write:
        url = AZURE_DB_BASE_URL.format(
            uid=os.getenv("DSCI_AZ_DB_DEV_UID_WRITE"),
            pw=os.getenv("DSCI_AZ_DB_DEV_PW_WRITE"),
            host="chd-rasterstats-dev2.postgres.database.azure.com",
        )
    else:
        url = AZURE_DB_BASE_URL.format(
            uid=os.getenv("DSCI_AZ_DB_DEV_UID"),
            pw=os.getenv("DSCI_AZ_DB_DEV_PW"),
            host="chd-rasterstats-dev2.postgres.database.azure.com",
        )
    return create_engine(url)
```

```python
query = """
SELECT *
FROM projects.ds_aa_nga_flooding_monitoring
LIMIT 100
"""
```

```python
df = pd.read_sql(query, get_backup_db())
```

```python
df
```
