-- created_at: 2026-02-16T23:16:58.169906572+00:00
-- finished_at: 2026-02-16T23:16:59.415048852+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: 6w1sUn6Q5Rb8RmeB0LKk9IH9pe6
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "ny_taxi_dbt", "target_name": "dev"} */

    select distinct schema_name from `project-481a6090-10e3-4ce0-9bf`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-16T23:17:01.573454842+00:00
-- finished_at: 2026-02-16T23:17:02.689507837+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: bigquery
-- node_id: model.ny_taxi_dbt.stg_fhv_tripdata
-- query_id: UtaSxFgzIymm70z0WWYmBQ1sAGX
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `project-481a6090-10e3-4ce0-9bf`.`m4_nytaxi`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-16T23:17:02.699422283+00:00
-- finished_at: 2026-02-16T23:17:03.718669137+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: bigquery
-- node_id: model.ny_taxi_dbt.stg_fhv_tripdata
-- query_id: sXH4o2Q9aOvFZiN3sRsHkhc5Ge0
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ny_taxi_dbt.stg_fhv_tripdata", "profile_name": "ny_taxi_dbt", "target_name": "dev"} */


  create or replace view `project-481a6090-10e3-4ce0-9bf`.`m4_nytaxi`.`stg_fhv_tripdata`
  OPTIONS()
  as with tripdata as (
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

select * from renamed;

;
