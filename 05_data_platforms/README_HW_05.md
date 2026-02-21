# Module 5: Data Platforms with Bruin Homework

### Setup Steps Completed

1. Installed Bruin CLI
2. Initialized zoomcamp template pipeline
3. Configured `.bruin.yml` with DuckDB connection
4. Built and ran full NYC taxi pipeline from the tutorial in the main module README.

---

## Question 1: Bruin Pipeline Structure

**Question:** In a Bruin project, what are the required files/directories?

**Answer:** .bruin.yml and pipeline.yml (assets can be anywhere)

**Explanation:**
 .bruin.yml and pipeline.yml (assets can be anywhere) — not the one with the specific pipeline/ and assets/ structure, because that's just how the zoomcamp template organized things, not a Bruin requirement.

---

## Question 2: Materialization Strategies

**Question:** You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

**Answer:** time_interval - incremental based on a time column

**Explanation:**
The `time_interval` strategy deletes all rows within a given time window and re-inserts the query results for that same window. This is ideal for time partitioned data like NYC taxi trips, allowing clean reruns for any specific month without touching other data.

---

## Question 3: Pipeline Variables

**Question:** You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

**Answer:** bruin run --var 'taxi_types=["yellow"]'

**Explanation:**
Array variables must be passed as valid JSON with proper shell quoting — outer single quotes protect the brackets from shell interpretation. Other options fail because they use invalid flags or don't format the value as a JSON array.

---

## Question 4: Running with Dependencies

**Question:** You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

**Answer:** bruin run ingestion/trips.py --downstream

**Explanation:**
The --downstream flag tells Bruin to run the specified asset plus all downstream assets that depend on it. Assets are referenced by file path, and the flag is confirmed in the bruin run --help output.
---

## Question 5: Quality Checks

**Question:** You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

**Answer:** not_null: true

**Explanation:**
The `not_null` check validates that no NULL values exist in the column and runs automatically after each asset completes. It was used in `staging.trips` for `pickup_datetime` and in `payment_lookup.asset.yml` for both ID and name columns.

```yaml
columns:
  - name: pickup_datetime
    type: timestamp
    checks:
      - name: not_null
```

---

## Question 6: Lineage and Dependencies

**Question:** After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

**Answer:** bruin lineage

**Explanation:**
The `bruin lineage` command visualizes the dependency graph between all assets in the pipeline. In the NYC taxi pipeline, ingestion assets run first in parallel, then staging, then reports.

---

## Question 7: First-Time Run

**Question:** You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

**Answer:** --full-refresh

**Explanation:**
The `--full-refresh` flag forces Bruin to create or recreate all tables from scratch, ignoring any incremental state. Without it, incremental strategies like `time_interval` will fail trying to DELETE from tables that don't exist yet.

```bash
bruin run ./zoomcamp/pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-02-01 --full-refresh
```

---

## Resources

- Bruin Documentation: https://getbruin.com
- DataTalks.Club Data Engineering Zoomcamp: https://github.com/DataTalksClub/data-engineering-zoomcamp
