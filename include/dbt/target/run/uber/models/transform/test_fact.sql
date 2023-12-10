
  
    

    create or replace table `uber-data-engineering-407103`.`uber`.`test_fact`
    
    

    OPTIONS()
    as (
      with datetime_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`datetime_dim`

),
dropoff_location_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`dropoff_location_dim`

),
pickup_location_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`pickup_location_dim`

),
passenger_count_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`passenger_count_dim`

),
payment_type_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`payment_type_dim`

),
rate_code_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`rate_code_dim`

),
trip_distance_cte as (

    select * from `uber-data-engineering-407103`.`uber`.`trip_distance`

),
ride_cte as (
    SELECT 
        CAST(ROW_NUMBER() OVER () AS INT64) as trip_id,
        VendorID,
        datetime_cte.datetime_id,
        passenger_count_cte.passenger_count_id,
        trip_distance_cte.trip_distance_id,
        rate_code_cte.rate_code_id,
        store_and_fwd_flag,
        pickup_location_cte.pickup_location_id,
        dropoff_location_cte.dropoff_location_id,
        payment_type_cte.payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM `uber-data-engineering-407103`.`uber`.`uber_tlc_trip_recrord_data`
    JOIN datetime_cte ON ride_cte.datetime_id = datetime_cte.datetime_id
    JOIN dropoff_location_cte ON ride_cte.dropoff_location_id = dropoff_location_cte.dropoff_location_id
    JOIN pickup_location_cte ON ride_cte.pickup_location_id = pickup_location_cte.pickup_location_id
    JOIN passenger_count_cte ON ride_cte.passenger_count_id = passenger_count_cte.passenger_count_id
    JOIN payment_type_cte ON ride_cte.payment_type_id = payment_type_cte.payment_type_id
    JOIN rate_code_cte ON ride_cte.rate_code_id = rate_code_cte.rate_code_id
    JOIN trip_distance_cte ON ride_cte.trip_distance_id = trip_distance_cte.trip_distance_id
)
select * from ride_cte
    );
  