WITH datetime_cte AS (  
  SELECT DISTINCT 
    to_hex(md5(cast(coalesce(cast(tpep_pickup_datetime as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tpep_dropoff_datetime as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS datetime_id,
    CASE
      WHEN LENGTH(tpep_pickup_datetime) = 16 THEN
        -- Date format: "DD/MM/YYYY HH:MM"
        PARSE_DATETIME('%m/%d/%Y %H:%M', tpep_pickup_datetime)
      WHEN LENGTH(tpep_pickup_datetime) <= 14 THEN
        -- Date format: "MM/DD/YY HH:MM"
        PARSE_DATETIME('%m/%d/%y %H:%M', tpep_pickup_datetime)
      ELSE
        NULL
    END AS pick_date_part,
    tpep_dropoff_datetime AS drop_datetime_id,
    CASE
      WHEN LENGTH(tpep_dropoff_datetime) = 16 THEN
        -- Date format: "DD/MM/YYYY HH:MM"
        PARSE_DATETIME('%m/%d/%Y %H:%M', tpep_dropoff_datetime)
      WHEN LENGTH(tpep_dropoff_datetime) <= 14 THEN
        -- Date format: "MM/DD/YY HH:MM"
        PARSE_DATETIME('%m/%d/%y %H:%M', tpep_dropoff_datetime)
      ELSE
        NULL
    END AS drop_date_part
  FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
  WHERE tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
)
SELECT
  datetime_id,
  pick_date_part AS tpep_pickup_datetime,
  drop_date_part AS tpep_dropoff_datetime,
  EXTRACT(HOUR FROM pick_date_part) AS pick_hour,
  EXTRACT(DAY FROM pick_date_part) AS pick_day,
  EXTRACT(MONTH FROM pick_date_part) AS pick_month,
  EXTRACT(YEAR FROM pick_date_part) AS pick_year,
  EXTRACT(DAYOFWEEK FROM pick_date_part) AS pick_weekday,
  EXTRACT(HOUR FROM drop_date_part) AS drop_hour,
  EXTRACT(DAY FROM drop_date_part) AS drop_day,
  EXTRACT(MONTH FROM drop_date_part) AS drop_month,
  EXTRACT(YEAR FROM drop_date_part) AS drop_year,
  EXTRACT(DAYOFWEEK FROM drop_date_part) AS drop_weekday
FROM datetime_cte