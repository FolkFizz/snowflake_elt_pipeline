-- =============================================================================
-- stg_trips.sql
--
-- Description:
-- This model cleans, standardizes, and unions the raw yellow and green taxi
-- trip data into a single staging model. It handles schema changes by
-- extracting data from the VARIANT column and harmonizing different schemas.
--
-- Materialization:
-- This model is materialized as a view for efficiency, as it doesn't store
-- data itself but calculates the transformation on the fly.
-- =============================================================================

{{ config(materialized='view') }}

WITH yellow_trip_data AS (
    SELECT
        -- Common columns
        raw_record:VendorID::INT AS vendor_id,
        raw_record:passenger_count::FLOAT AS passenger_count, -- Cast as FLOAT to match green taxi data
        raw_record:trip_distance::FLOAT AS trip_distance,
        raw_record:RatecodeID::FLOAT AS rate_code_id, -- Cast as FLOAT to match green taxi data
        raw_record:store_and_fwd_flag::VARCHAR AS store_and_fwd_flag,
        raw_record:PULocationID::INT AS pickup_location_id,
        raw_record:DOLocationID::INT AS dropoff_location_id,
        raw_record:payment_type::INT AS payment_type,
        raw_record:fare_amount::FLOAT AS fare_amount,
        raw_record:extra::FLOAT AS extra_surcharge,
        raw_record:mta_tax::FLOAT AS mta_tax,
        raw_record:tip_amount::FLOAT AS tip_amount,
        raw_record:tolls_amount::FLOAT AS tolls_amount,
        raw_record:improvement_surcharge::FLOAT AS improvement_surcharge,
        raw_record:total_amount::FLOAT AS total_amount,
        raw_record:congestion_surcharge::FLOAT AS congestion_surcharge,

        -- Yellow-specific columns, aliased to a standard name
        raw_record:tpep_pickup_datetime::TIMESTAMP AS pickup_datetime,
        raw_record:tpep_dropoff_datetime::TIMESTAMP AS dropoff_datetime,

        -- Columns that do not exist in Yellow taxi data, created as NULL/default
        NULL::FLOAT AS ehail_fee,
        NULL::INT AS trip_type,

        -- Columns that may or may not exist depending on the year
        COALESCE(raw_record:airport_fee::FLOAT, 0) AS airport_fee,
        COALESCE(raw_record:cbd_congestion_fee::FLOAT, 0) AS cbd_congestion_fee

    FROM {{ source('nyc_taxi_data', 'RAW_YELLOW_TRIPS') }}
),

green_trip_data AS (
    SELECT
        -- Common columns
        raw_record:VendorID::INT AS vendor_id,
        raw_record:passenger_count::FLOAT AS passenger_count,
        raw_record:trip_distance::FLOAT AS trip_distance,
        raw_record:RatecodeID::FLOAT AS rate_code_id,
        raw_record:store_and_fwd_flag::VARCHAR AS store_and_fwd_flag,
        raw_record:PULocationID::INT AS pickup_location_id,
        raw_record:DOLocationID::INT AS dropoff_location_id,
        raw_record:payment_type::FLOAT::INT AS payment_type, -- Cast to FLOAT then INT to handle potential decimals
        raw_record:fare_amount::FLOAT AS fare_amount,
        raw_record:extra::FLOAT AS extra_surcharge,
        raw_record:mta_tax::FLOAT AS mta_tax,
        raw_record:tip_amount::FLOAT AS tip_amount,
        raw_record:tolls_amount::FLOAT AS tolls_amount,
        raw_record:improvement_surcharge::FLOAT AS improvement_surcharge,
        raw_record:total_amount::FLOAT AS total_amount,
        raw_record:congestion_surcharge::FLOAT AS congestion_surcharge,

        -- Green-specific columns, aliased to a standard name
        raw_record:lpep_pickup_datetime::TIMESTAMP AS pickup_datetime,
        raw_record:lpep_dropoff_datetime::TIMESTAMP AS dropoff_datetime,

        -- Columns that exist only in Green taxi data
        raw_record:ehail_fee::FLOAT AS ehail_fee,
        raw_record:trip_type::FLOAT::INT AS trip_type,

        -- Columns that may or may not exist depending on the year
        NULL::FLOAT AS airport_fee, -- airport_fee is not in green taxi data
        COALESCE(raw_record:cbd_congestion_fee::FLOAT, 0) AS cbd_congestion_fee

    FROM {{ source('nyc_taxi_data', 'RAW_GREEN_TRIPS') }}
),

unioned_data AS (
    SELECT
        'Yellow' AS taxi_color,
        *
    FROM yellow_trip_data

    UNION ALL

    SELECT
        'Green' AS taxi_color,
        *
    FROM green_trip_data
)

SELECT * FROM unioned_data

