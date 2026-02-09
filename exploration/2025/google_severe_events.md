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

# Google severe events exploration
<!-- markdownlint-disable MD013 -->
Using the Colab notebook [here](https://colab.research.google.com/drive/1WbiSF4bvjSLdAEnYaBgw1LrL_Vf_cB9Y#scrollTo=e0PLcJaUdnJg) as a reference.

REST API reference [here](https://developers.google.com/flood-forecasting/rest).

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import io
import os

import requests
import geopandas as gpd

from src.datasources import worldpop
```

```python
from dotenv import load_dotenv

load_dotenv()
```

```python
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BASE_URL = "https://floodforecasting.googleapis.com/v1"
```

## Get severe events

```python
severe_events = []
request = {}

while True:
    res = requests.post(
        f"{BASE_URL}/severeEvents:searchLatestSevereEvents?key={GOOGLE_API_KEY}",
        json=request,
    )
    severe_events_response = res.json()
    if "error" in severe_events_response:
        print(severe_events_response)
        break
    severe_events.extend(severe_events_response.get("severeEvents", []))
    nextPageToken = severe_events_response.get("nextPageToken")
    if not nextPageToken:
        break
    request["pageToken"] = nextPageToken
```

```python
severe_events[0]
```

```python
len(severe_events[0]["gaugeIds"])
```

### Get polygon

```python
if not severe_events:
    print("No severe events found")
else:
    event = severe_events[0]
    serialized_polygon_response = requests.get(
        f"{BASE_URL}/serializedPolygons/{event['eventPolygonId']}?key={GOOGLE_API_KEY}"
    ).json()
    if "error" in serialized_polygon_response:
        print(
            f'Error fetching polygon: {serialized_polygon_response["error"]}'
        )
    else:
        with open(f"temp/event_polygon.kml", "w") as f:
            f.write(serialized_polygon_response["kml"])
```

```python
kml_text = serialized_polygon_response["kml"]
```

```python
gdf
```

```python
gdf = gpd.read_file(io.BytesIO(kml_text.encode("utf-8")), driver="LIBKML")
ax = gdf.boundary.plot(figsize=(7, 6))
```

## Check exposure

Let's just see how close their exposure numbers are to what we'd estimate with WorldPop

```python
worldpop.download_worldpop_to_blob(iso3="mli")
```

```python
worldpop.download_worldpop_to_blob(iso3="gin")
```

```python
wp_mli = worldpop.load_worldpop_from_blob(iso3="mli")
```

```python
wp_gin = worldpop.load_worldpop_from_blob(iso3="gin")
```

```python
wp_mli_clip = wp_mli.rio.clip(gdf.geometry)
```

```python
mli_sum = float(wp_mli_clip.sum())
```

```python
mli_sum
```

```python
wp_gin_clip = wp_gin.rio.clip(gdf.geometry)
```

```python
gin_sum = float(wp_gin_clip.sum())
```

```python
gin_sum
```

```python
gin_sum + mli_sum
```

## Check discovery

Just to see if there are any other relevant resources or methods

```python
def check_discovery():
    """Fetch and print methods from the discovery document."""
    url = f"https://floodforecasting.googleapis.com/$discovery/rest?version=v1&key={GOOGLE_API_KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    doc = r.json()

    print("=== Discovery document methods ===")
    # Resources and their methods
    for resource, details in doc.get("resources", {}).items():
        print("Resource:", resource)
        for method_name in details.get("methods", {}):
            print("   -", method_name)

    # Top-level methods
    for method_name in doc.get("methods", {}):
        print("Top-level method:", method_name)
```

```python
check_discovery()
```
