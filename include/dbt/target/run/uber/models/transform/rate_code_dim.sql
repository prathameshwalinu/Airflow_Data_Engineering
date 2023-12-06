
  
    

    create or replace table `uber-data-engineering-407103`.`uber`.`rate_code_dim`
    
    

    OPTIONS()
    as (
      WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as rate_code_id,
    RatecodeID AS RatecodeID,
    ROW_NUMBER() OVER (ORDER BY RatecodeID) as row_num
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
  WHERE RatecodeID IS NOT NULL
)
SELECT
  rate_code_id,
  RatecodeID
FROM numbered_rows
    );
  