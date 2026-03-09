# Module 6 Homework: Batch Processing with Spark

## DE Zoomcamp 2026 | Dataset: NYC Yellow Taxi — November 2025
---

## Data Sources

Download the November 2025 Yellow Taxi data:
```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

Download the zone lookup data:
```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

---

## Question 1: Install Spark and PySpark

Install Spark, run PySpark, create a local Spark session, and execute `spark.version`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("homework") \
    .getOrCreate()

print(spark.version)
```

**Answer: 4.1.1**

---

## Question 2: Yellow November 2025

Read the data into a Spark DataFrame, repartition to 4 partitions, and save as parquet. What is the average file size?

```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.parquet("output/", mode="overwrite")
```

**Answer: 25 MB**

---

## Question 3: Count Records on November 15th

How many taxi trips started on November 15, 2025?

```python
from pyspark.sql.functions import to_date

df.filter(to_date(df.tpep_pickup_datetime) == "2025-11-15").count()
```

**Answer: 162,604**

---

## Question 4: Longest Trip in Hours

What is the length of the longest trip in the dataset in hours?

```python
from pyspark.sql.functions import unix_timestamp, max

df.withColumn("duration_hours",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
).select(max("duration_hours")).show()
```

**Answer: 90.6**

---

## Question 5: Spark UI Port

Spark's User Interface which shows the application's dashboard runs on which local port?

**Answer: 4040**

---

## Question 6: Least Frequent Pickup Location Zone

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location zone?

```python
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
zones.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

spark.sql("""
    SELECT z.Zone, COUNT(*) as trip_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 1
""").show()
```

**Answer: Arden Heights**

---

## Full hw6.py Script

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('hw6') \
    .getOrCreate()

# Q1
print("Spark version:", spark.version)

# Read data
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

# Q2 - Repartition and save
df.repartition(4).write.parquet('yellow_nov_2025/', mode='overwrite')
files = [f for f in os.listdir('yellow_nov_2025/') if f.endswith('.parquet')]
sizes = [os.path.getsize(f'yellow_nov_2025/{f}') / (1024*1024) for f in files]
print(f"Q2 - Avg file size: {sum(sizes)/len(sizes):.2f} MB")

# Q3 - Count trips on Nov 15
nov15 = df.filter(F.to_date(df.tpep_pickup_datetime) == '2025-11-15')
print(f"Q3 - Trips on Nov 15: {nov15.count()}")

# Q4 - Longest trip in hours
df2 = df.withColumn('duration_hrs',
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600)
print(f"Q4 - Longest trip (hrs): {df2.agg(F.max('duration_hrs')).collect()[0][0]:.1f}")

# Q6 - Least frequent pickup zone
zones = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')
df.groupBy('PULocationID').count() \
    .join(zones, df.PULocationID == zones.LocationID) \
    .orderBy('count') \
    .select('Zone', 'count') \
    .show(1)
```
