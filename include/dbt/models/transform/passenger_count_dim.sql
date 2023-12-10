WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as passenger_count_id,
    passenger_count AS passenger_count,
    ROW_NUMBER() OVER () as row_num
  FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
  WHERE passenger_count IS NOT NULL
)
SELECT
  passenger_count_id,
  passenger_count
FROM numbered_rows