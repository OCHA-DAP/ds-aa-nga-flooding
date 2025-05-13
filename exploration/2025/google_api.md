---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.7
  kernelspec:
    display_name: nga-flooding
    language: python
    name: nga-flooding
---

# Google Flood Forecast API

Exploring basic functionality of the Flood Forecast API. See the following resources:

- https://colab.research.google.com/drive/1et0vjN9coLck11YhORyprgtOvCuaErVd?usp=sharing#scrollTo=omVd-Y7OJePs
- https://developers.google.com/flood-forecasting/rest/v1/gauges/queryGaugeForecasts
- https://developers.google.com/flood-forecasting

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from dotenv import load_dotenv
import datetime
import os
import requests
import pandas as pd
from src.datasources import grrr
```

Lets start by getting some metadata for our gauge. See [here](https://developers.google.com/flood-forecasting/rest/v1/floodStatus#resource:-floodstatus) for response schema details.

```python
flood_status = requests.get(
    "https://floodforecasting.googleapis.com/v1/floodStatus:queryLatestFloodStatusByGaugeIds",
    params={
        "key": grrr.FLOODS_API_KEY,
        "gaugeIds": grrr.HYBAS_ID,
    },
).json()
```

We can see some basic information here about the flood status at this gauge. As of the time this update was issued, there is no flooding at this gauge.

```python
flood_status["floodStatuses"]
```

Now imagine we want to check the latest forecasts for our gauge.

```python
today = datetime.datetime.now()

res = requests.get(
    "https://floodforecasting.googleapis.com/v1/gauges:queryGaugeForecasts",
    params={
        "key": grrr.FLOODS_API_KEY,
        "gaugeIds": grrr.HYBAS_ID,
        "issuedTimeStart": today.strftime("%Y-%m-%d"),
    },
).json()
```

```python
rows = []

for forecast in res["forecasts"][HYBAS_ID]["forecasts"]:
    issued_time = forecast["issuedTime"]
    gauge_id = forecast["gaugeId"]

    # Process each forecast range
    for range_item in forecast["forecastRanges"]:
        row = {
            "issuedTime": issued_time,
            "gaugeId": gauge_id,
            "value": range_item["value"],
            "forecastStartTime": range_item["forecastStartTime"],
            "forecastEndTime": range_item["forecastEndTime"],
        }
        rows.append(row)

# Create the DataFrame
df = pd.DataFrame(rows)

# Convert string timestamps to datetime objects for easier analysis
df["issuedTime"] = pd.to_datetime(df["issuedTime"])
df["forecastStartTime"] = pd.to_datetime(df["forecastStartTime"])
df["forecastEndTime"] = pd.to_datetime(df["forecastEndTime"])
```

Note that the validity time for some of the forecast data is *before* the issue time? See note from the docs [here](https://developers.google.com/flood-forecasting/rest/v1/gauges/queryGaugeForecasts#forecast).

> Note: Some of the forecast ranges can potentially be earlier than the issued time. This can happen due to e.g., lags in input data for the model. With the above example, it could be that the issue time is 5pm, and the forecast ranges are for 4pm, 5pm, 6pm, etc.

```python
df
```

Let's add a lead time as well

```python
# Calculate leadtime in hours (time between when forecast was issued and the forecast start time)
df["leadtime_hours"] = (
    df["forecastStartTime"] - df["issuedTime"]
).dt.total_seconds() / 3600
df["leadtime_days"] = (
    df["forecastStartTime"] - df["issuedTime"]
).dt.total_seconds() / (24 * 3600)
df = df.sort_values(["issuedTime", "forecastStartTime"])
```

The timing of when forecasts are issued is also interesting. It looks like there were two separate forecasts issued today. Would this be the same issue as above relating to lags in input data for the model?

```python
df
```
