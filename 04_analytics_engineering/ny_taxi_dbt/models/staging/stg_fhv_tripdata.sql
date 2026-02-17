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