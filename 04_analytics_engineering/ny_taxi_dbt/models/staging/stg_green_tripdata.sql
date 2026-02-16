with tripdata as (
    select * 
    from `project-481a6090-10e3-4ce0-9bf.m4_nytaxi.green_tripdata`
    where vendorid is not null 
),

renamed as (
  select
      -- identifiers
      cast(vendorid as integer) as vendor_id,
      cast(ratecodeid as integer) as ratecode_id,
      cast(pulocationid as integer) as pickup_location_id,
      cast(dolocationid as integer) as dropoff_location_id,
      
      -- timestamps
      cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
      
      -- trip info
      store_and_fwd_flag,
      cast(passenger_count as integer) as passenger_count,
      cast(trip_distance as numeric) as trip_distance,
      cast(trip_type as integer) as trip_type,
      
      -- payment info
      cast(fare_amount as numeric) as fare_amount,
      cast(extra as numeric) as extra,
      cast(mta_tax as numeric) as mta_tax,
      cast(tip_amount as numeric) as tip_amount,
      cast(tolls_amount as numeric) as tolls_amount,
      cast(ehail_fee as numeric) as ehail_fee,
      cast(improvement_surcharge as numeric) as improvement_surcharge,
      cast(total_amount as numeric) as total_amount,
      cast(payment_type as integer) as payment_type,
  from tripdata
)

select * from renamed