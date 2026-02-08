# Module 3: Data Warehousing & BigQuery Homework

## Setup Information

**GCP Project ID:** `project-481a6090-10e3-4ce0-9bf`  
**Dataset Name:** `taxi_data`  
**GCS Bucket:** `de_zoomcamp_2026_taxi_data_jt`  
**Data Source:** Yellow Taxi Trip Records (January 2024 - June 2024)

---

## Question 1: Counting Records

**Question:** What is count of records for the 2024 Yellow Taxi Data?

**Answer:** 20,332,093

**Query:**
```
select count(*)
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
```

---

## Question 2: Data Read Estimation

**Question:** 
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? 

**Answer:** 0 MB for the External Table and 155.12 MB for the Materialized Table

**Query for External Table:**
```
select count(DISTINCT PULocationID)
from `project-481a6090-10e3-4ce0-9bf.taxi_data.external_yellow_taxi`
```
**Estimated Bytes (External Table):** O MB

**Query for Regular Table:**
```
select count(DISTINCT PULocationID)
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
```
**Estimated Bytes (Regular Table):** 155.12 MB

---

## Question 3: Understanding Columnar Storage

**Question:** 
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

Why are the estimated number of Bytes different? 

**Answer:** 
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

**Query for One Column:**
```
select PULocationID
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
```
**Estimated Bytes (1 column):** 155.12 MB

**Query for Two Columns:**
```
select PULocationID,DOLocationID
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
```
**Estimated Bytes (2 columns):** 310.24 MB

---

## Question 4: Counting Zero Fare Trips

**Question:** How many records have a fare_amount of 0?

**Answer:** 8,333

**Query:**
```
select count(*)
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
where fare_amount = 0;
```

---

## Question 5: Partitioning and Clustering

**Question:** What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)?

**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

**Query to Create Partitioned/Clustered Table:**
```
CREATE OR REPLACE TABLE `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`;

```
**Reasoning:** 
Partition on what you FILTER by (WHERE clauses). Cluster on what you GROUP/ORDER by

---

## Question 6: Partition Benefits

**Question:** Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

**Answer:** 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

**Query for Non-Partitioned Table:**
```
select DISTINCT VendorID
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi`
where DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'

```
**Estimated Bytes (Non-Partitioned):** 310.24 MB

**Query for Partitioned Table:**
```
select DISTINCT VendorID
from `project-481a6090-10e3-4ce0-9bf.taxi_data.yellow_taxi_partitioned`
where DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'

```
**Estimated Bytes (Partitioned):** 26.84 MB

---

## Question 7: External Table Storage

**Question:** Where is the data stored in the External Table you created?

**Answer:** GCP Bucket

**Reasoning:**
External tables don't store data in BigQuery. They reference data in GCS buckets- therefore the data is stored in a GCP Bucket for the external table I created.

---

## Question 8: Clustering Best Practices

**Question:** It is best practice in Big Query to always cluster your data - True or False?

**Answer:** False

**Reasoning:**
Tables with less than 1GB don't show significant improvement with partitioning and clustering; doing so in a small table could even lead to increased cost due to the additional metadata reads and maintenance needed for these features.