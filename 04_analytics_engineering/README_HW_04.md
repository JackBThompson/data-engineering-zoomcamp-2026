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

**Answer:** 

**Explanation:**

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

**Answer:** 

**Explanation:**

---

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**Question:** After running your dbt project, query the `fct_monthly_zone_revenue` model. What is the count of records in the `fct_monthly_zone_revenue` model?

**Answer:** 

**Query:**
```sql

```

**Result:**

---

## Question 4: Best Performing Zone for Green Taxis (2020)

**Question:** Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020. Which zone had the highest revenue?

**Answer:** 

**Query:**
```sql

```

**Result:**

---

## Question 5: Green Taxi Trip Counts (October 2019)

**Question:** Using the `fct_monthly_zone_revenue` table, what is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?

**Answer:** 

**Query:**
```sql

```

**Result:**

---

## Question 6: Build a Staging Model for FHV Data

**Question:** Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.

Requirements:
1. Load the FHV trip data for 2019 into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

**Answer:** 

**stg_fhv_tripdata.sql Code:**
```sql

```

**Query to Count Records:**
```sql

```

**Result:**

**Notes on Implementation:**

---

## Summary of Key Learnings

**dbt Lineage and Dependencies:**


**dbt Testing:**


**Analytics Engineering Best Practices:**


**Working with FHV Data:**


---

## Resources

- dbt Documentation: https://docs.getdbt.com/
- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- DataTalks.Club Data Engineering Zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp
