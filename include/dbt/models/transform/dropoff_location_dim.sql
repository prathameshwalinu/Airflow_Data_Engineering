WITH numbered_rows AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['dropoff_latitude','dropoff_longitude'])}} as dropoff_location_id,
    dropoff_latitude AS dropoff_latitude,
    dropoff_longitude AS dropoff_longitude,
    ROW_NUMBER() OVER (ORDER BY dropoff_latitude, dropoff_longitude) as row_num
  FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
)
SELECT
  dropoff_location_id,
  dropoff_latitude,
  dropoff_longitude
FROM numbered_rows