WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as payment_type_id,
    payment_type AS payment_type,
    ROW_NUMBER() OVER (ORDER BY payment_type) as row_num
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
  WHERE payment_type IS NOT NULL
)
SELECT
  payment_type_id,
  payment_type
FROM numbered_rows