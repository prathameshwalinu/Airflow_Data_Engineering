
  
    

    create or replace table `uber-data-engineering-407103`.`uber`.`trip_distance`
    
    

    OPTIONS()
    as (
      WITH numbered_rows AS (
  SELECT
    GENERATE_UUID() as trip_distance_id,
    trip_distance AS trip_distance,
    ROW_NUMBER() OVER (ORDER BY trip_distance) as row_num
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
  WHERE trip_distance IS NOT NULL
)
SELECT
  trip_distance_id,
  trip_distance
FROM numbered_rows
    );
  