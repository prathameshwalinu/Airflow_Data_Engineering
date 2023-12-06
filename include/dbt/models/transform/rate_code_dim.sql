WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as rate_code_id,
    RatecodeID AS RatecodeID,
    ROW_NUMBER() OVER (ORDER BY RatecodeID) as row_num
  FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
  WHERE RatecodeID IS NOT NULL
)
SELECT
  rate_code_id,
  RatecodeID
FROM numbered_rows
