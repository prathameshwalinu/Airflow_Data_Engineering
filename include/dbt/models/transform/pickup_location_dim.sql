WITH numbered_rows AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['pickup_latitude','pickup_longitude']) }} as pickup_location_id,
    pickup_latitude AS pickup_latitude,
    pickup_longitude AS pickup_longitude,
    ROW_NUMBER() OVER (ORDER BY pickup_latitude, pickup_longitude) as row_num
  FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
)
SELECT
  pickup_location_id,
  pickup_latitude,
  pickup_longitude
FROM numbered_rows