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

# Wuroboki

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import nihsa
```

```python
df = nihsa.load_wuroboki()
df
```

```python
df.sort_values("level")
```

```python
df.groupby(df["time"].dt.year)["level"].max().reset_index().sort_values(
    "level"
)
```

```python

```
