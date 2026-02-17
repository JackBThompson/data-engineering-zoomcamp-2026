/*
One row per trip — yellow and green combined (the union is already done in the intermediate model)
Add a primary key (trip_id) — it has to be unique
Find and fix duplicates — there are quite a few in this dataset. Some come from the source, some get introduced during the union. Find them, understand why they happen, and fix them
Enrich payment_type (there is a seed for this in the repo).
*/

-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.ratecode_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    timestamp_diff(trips.dropoff_datetime, trips.pickup_datetime, minute) as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description  -- ← Just pass through from int_trips

from {{ ref('int_trips') }} as trips
left join {{ ref('dim_zones') }} as pz
    on trips.pickup_location_id = pz.location_id
left join {{ ref('dim_zones') }} as dz
    on trips.dropoff_location_id = dz.location_id
-- REMOVED the payment type join since int_trips already has it

{% if is_incremental() %}
  where trips.pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}