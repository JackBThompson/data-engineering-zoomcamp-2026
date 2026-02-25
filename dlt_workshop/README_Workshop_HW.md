# Workshop 1: Data Ingestion with dlt — Homework

## Overview
This homework builds a dlt pipeline that loads NYC Yellow Taxi trip data from a custom API into DuckDB, then queries the data to answer three questions.

---

## Data Source

**API URL** `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` |

---

## Setup

### Install dlt
```bash
pip install "dlt[workspace]"
```

### Initialize the project
```bash
dlt init dlthub:taxi_pipeline duckdb
```

### Pipeline code (`taxi_pipeline.py`)
```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides")
def taxi_data():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

info = pipeline.run(taxi_data(), write_disposition="replace")
print(info)
```

### Run the pipeline
```bash
python taxi_pipeline.py
```

---

## Homework Questions & Queries

### Question 1: What is the start date and end date of the dataset?

```python
import duckdb
conn = duckdb.connect('taxi_pipeline.duckdb')

conn.execute('''
    SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) 
    FROM ny_taxi_data.rides
''').fetchall()
```

**Answer:** `2009-06-01 to 2009-07-01`

---

### Question 2: What proportion of trips are paid with credit card?

```python
import duckdb
conn = duckdb.connect('taxi_pipeline.duckdb')

conn.execute("""
    SELECT ROUND(COUNT(*) FILTER (WHERE payment_type = 'Credit') * 100.0 / COUNT(*), 2)
    FROM ny_taxi_data.rides
""").fetchall()
```

**Note:** `payment_type` is stored as text (e.g. `'Credit'`), not an integer.

**Answer:** `26.66%`

---

### Question 3: What is the total amount of money generated in tips?

```python
import duckdb
conn = duckdb.connect('taxi_pipeline.duckdb')

conn.execute('''
    SELECT ROUND(SUM(tip_amt), 2) 
    FROM ny_taxi_data.rides
''').fetchall()
```

**Answer:** `$6,063.41`

---
