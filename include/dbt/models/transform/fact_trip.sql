WITH fct_invoices_cte AS (
    SELECT
        CAST(ROW_NUMBER() OVER () AS INT64) as trip_id,
        VendorID,
        {{ dbt_utils.generate_surrogate_key(['tpep_pickup_datetime','tpep_dropoff_datetime']) }} AS datetime_id,
        passenger_count_id,
        trip_distance_id,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        {{ dbt_utils.generate_surrogate_key(['dropoff_latitude','dropoff_longitude']) }} as dropoff_location_id,
        payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
)
SELECT
    trip_id,
    VendorID,
    datetime_id,
    do.dropoff_location_id,
    pu.pickup_location_id,
    pc.passenger_count_id,
    pt.payment_type_id,
    rc.rate_code_id,
    store_and_fwd_flag,
    td.trip_distance_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge AS improvement_surcharge_amount,  -- Added alias
    total_amount
FROM fct_invoices_cte fi
INNER JOIN {{ ref('datetime_dim') }} dt ON fi.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dropoff_location_dim') }} do ON fi.dropoff_location_id = do.dropoff_location_id
INNER JOIN {{ ref('pickup_location_dim') }} pu ON fi.pickup_location_id = pu.pickup_location_id
INNER JOIN {{ ref('passenger_count_dim') }} pc ON fi.passenger_count_id = pc.passenger_count_id
INNER JOIN {{ ref('payment_type_dim') }} pt ON fi.payment_type_id = pt.payment_type_id
INNER JOIN {{ ref('rate_code_dim') }} rc ON fi.rate_code_id = rc.rate_code_id
INNER JOIN {{ ref('trip_distance') }} td ON fi.trip_distance_id = td.trip_distance_id