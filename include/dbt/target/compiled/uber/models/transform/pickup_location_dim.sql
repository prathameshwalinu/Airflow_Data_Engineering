WITH numbered_rows AS (
  SELECT
    to_hex(md5(cast(coalesce(cast(pickup_latitude as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(pickup_longitude as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as pickup_location_id,
    pickup_latitude AS pickup_latitude,
    pickup_longitude AS pickup_longitude,
    ROW_NUMBER() OVER (ORDER BY pickup_latitude, pickup_longitude) as row_num
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
)
SELECT
  pickup_location_id,
  pickup_latitude,
  pickup_longitude
FROM numbered_rows