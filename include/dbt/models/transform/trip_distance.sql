WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as trip_distance_id,
    trip_distance AS trip_distance,
    ROW_NUMBER() OVER (ORDER BY trip_distance) as row_num
  FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
  WHERE trip_distance IS NOT NULL
)
SELECT
  trip_distance_id,
  trip_distance
FROM numbered_rows
