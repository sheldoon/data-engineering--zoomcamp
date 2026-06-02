select
    -- identifiers
    vendorid,
    ratecodeid,
    pulocationid,
    dolocationid,

    -- timestamps
    lpep_pickup_datetime,
    lpep_dropoff_datetime,

    -- trip info

    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    
    -- payment info

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type


 from {{source('raw_data', 'green_tripdata')}}