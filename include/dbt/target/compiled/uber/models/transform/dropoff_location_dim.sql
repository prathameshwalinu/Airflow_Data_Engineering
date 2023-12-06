WITH numbered_rows AS (
  SELECT
    to_hex(md5(cast(coalesce(cast(dropoff_latitude as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dropoff_longitude as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as dropoff_location_id,
    dropoff_latitude AS dropoff_latitude,
    dropoff_longitude AS dropoff_longitude,
    ROW_NUMBER() OVER (ORDER BY dropoff_latitude, dropoff_longitude) as row_num
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
)
SELECT
  dropoff_location_id,
  dropoff_latitude,
  dropoff_longitude
FROM numbered_rows