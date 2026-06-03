select
    -- identifiers
    SAFE_CAST(SPLIT(vendor_id, '.')[OFFSET(0)] AS INT64) as vendor_id,
    SAFE_CAST(SPLIT(rate_code, '.')[OFFSET(0)] AS INT64) as rate_code_id,
    SAFE_CAST(SPLIT(pickup_location_id, '.')[OFFSET(0)] AS INT64) as pickup_location_id,
    SAFE_CAST(SPLIT(dropoff_location_id, '.')[OFFSET(0)] AS INT64) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as TIMESTAMP) as pickup_datetime,
    cast(dropoff_datetime as TIMESTAMP) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as INT64) as passenger_count,
    cast(trip_distance as NUMERIC) as trip_distance,
    SAFE_CAST(SPLIT(trip_type, '.')[OFFSET(0)] AS INT64) as trip_type,
    
    -- payment info
    cast(fare_amount as NUMERIC) as fare_amount,
    cast(extra as NUMERIC) as extra,
    cast(mta_tax as NUMERIC) as mta_tax,
    cast(tip_amount as NUMERIC) as tip_amount,
    cast(tolls_amount as NUMERIC) as tolls_amount,
    cast(imp_surcharge as NUMERIC) as improvement_surcharge,
    cast(total_amount as NUMERIC) as total_amount,
    SAFE_CAST(SPLIT(payment_type, '.')[OFFSET(0)] AS INT64) AS payment_type

from {{ source('raw_data', 'green_tripdata') }}
where vendor_id is not null