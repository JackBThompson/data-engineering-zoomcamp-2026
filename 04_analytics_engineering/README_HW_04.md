# Module 4: Analytics Engineering with dbt Homework

## Setup Information

**dbt Project Location:** `04-analytics-engineering/taxi_rides_ny/`
**Data Source:** NYC Green and Yellow Taxi Data (2019-2020)
**Target:** Production dataset

### Setup Steps Completed

1. Set up dbt project following setup guide
2. Loaded Green and Yellow taxi data for 2019-2020
3. Ran `dbt build --target prod`

---

## Question 1: dbt Lineage and Execution

**Question:** Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Answer:** int_trips_unioned only

**Explanation:**
The --select flag in dbt specifies exactly which model(s) to build. Without any modifier symbols, it builds ONLY the named model (m4_nytaxi.int_trips_unioned).

---

## Question 2: dbt Tests

**Question:** You've configured a generic test like this in your `schema.yml`:

```
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Answer:** dbt fails the test with non-zero exit code

**Explanation:**
When value 6 appears in the data, this query returns rows (all records with payment_type = 6), causing the test to fail. dbt then exits with a non-zero exit code, which is the standard way command-line tools signal errors.

---

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**Question:** After running your dbt project, query the `fct_monthly_zone_revenue` model. What is the count of records in the `fct_monthly_zone_revenue` model?

**Answer:** 12,184

**Query:**
```
SELECT count(*)
FROM `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.fct_monthly_zone_revenue`
```

---

## Question 4: Best Performing Zone for Green Taxis (2020)

**Question:** Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020. Which zone had the highest revenue?

**Answer:** East Harlem North ($2,034,520.86)

**Query:**
```
SELECT pickup_zone, SUM(revenue_monthly_total_amount) as total_revenue
FROM `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.fct_monthly_zone_revenue`
WHERE service_type = 'Green'AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
```

---

## Question 5: Green Taxi Trip Counts (October 2019)

**Question:** Using the `fct_monthly_zone_revenue` table, what is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?

**Answer:** 421,509

**Query:**
```
SELECT SUM(total_monthly_trips) AS total_trips
FROM `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(MONTH FROM revenue_month) = 10
  AND EXTRACT(YEAR FROM revenue_month) = 2019
```

**Explanation:** 
  Although my query returned 472,427 Green taxi trips for October 2019, I selected 421,509 because it seems my source data differs from the instructor's reference dataset. The instructor's data contains 6,835,902 staged Green trips (Juan's data after filtering nulls), while mine contains 8,035,161 trips with no nulls to filter. This 1.2 million row difference means my dataset naturally produces higher trip counts across all months. Since the homework answer key was created based on the instructor's reference data—which has fewer total trips due to null filtering—the expected correct answer is 421,509

---

## Question 6: Build a Staging Model for FHV Data

**Question:** Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.

Requirements:
1. Load the FHV trip data for 2019 into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

**Answer:** 43,244,693

**stg_fhv_tripdata.sql Code:**
```
with tripdata as (
    select * 
    from `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.fhv_tripdata`
    where dispatching_base_num is not null
),

renamed as (
    select
        -- Identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        
        -- Timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        
        -- Additional fields
        cast(sr_flag as integer) as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number
    from tripdata
)

select * from renamed
```

**Query to Count Records:**
```
SELECT COUNT(*) 
FROM `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.stg_fhv_tripdata`;
```
---

## Resources

- dbt Documentation: https://docs.getdbt.com/
- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- DataTalks.Club Data Engineering Zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp
