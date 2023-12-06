SELECT DISTINCT
        GENERATE_UUID() as passenger_count_id,
		passenger_count AS passenger_count,
FROM {{ source('uber', 'uber_tlc_trip_recrord_data') }}
WHERE passenger_count IS NOT NULL