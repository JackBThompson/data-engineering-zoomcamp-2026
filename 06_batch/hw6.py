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
zones = spark.read.option("header","true").csv('taxi_zone_lookup.csv')
df.groupBy('PULocationID').count() \
    .join(zones, df.PULocationID == zones.LocationID) \
    .orderBy('count') \
    .select('Zone', 'count') \
    .show(1)