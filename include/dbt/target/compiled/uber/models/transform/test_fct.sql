WITH fct_invoices_cte AS (
    SELECT
        CAST(ROW_NUMBER() OVER () AS INT64) as trip_id,
        VendorID,
        to_hex(md5(cast(coalesce(cast(tpep_pickup_datetime as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tpep_dropoff_datetime as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS datetime_id,
        passenger_count_id,
        trip_distance_id,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        to_hex(md5(cast(coalesce(cast(dropoff_latitude as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_longitude as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as dropoff_location_id,
        payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
)SELECT
    fi.trip_distance_id,
    td.trip_distance
FROM fct_invoices_cte fi
INNER JOIN `uber-data-engineering-407103`.`uber`.`trip_distance` td ON fi.trip_distance_id = td.trip_distance_id