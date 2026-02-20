"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import io

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start = datetime.fromisoformat(start_date[:10])
    end = datetime.fromisoformat(end_date[:10])

    dfs = []
    current = start
    while current < end:
        year = current.strftime("%Y")
        month = current.strftime("%m")
        for taxi_type in taxi_types:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
            print(f"Downloading: {url}")
            response = requests.get(url)
            if response.status_code == 200:
                df = pd.read_parquet(io.BytesIO(response.content))
                df["taxi_type"] = taxi_type
                dfs.append(df)
            else:
                print(f"Warning: could not fetch {url} (status {response.status_code})")
        current += relativedelta(months=1)

    final_dataframe = pd.concat(dfs, ignore_index=True)
    return final_dataframe