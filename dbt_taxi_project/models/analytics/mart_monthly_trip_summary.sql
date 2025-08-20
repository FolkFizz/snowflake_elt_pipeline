-- =============================================================================
-- mart_monthly_trip_summary.sql
--
-- Description:
-- This model creates a monthly aggregated summary of taxi trips. This is a
-- gold layer table, ready for business intelligence and analytics.
--
-- Materialization:
-- This model is materialized as a table because the aggregations can be
-- computationally expensive. Storing the results makes querying faster.
-- =============================================================================

{{ config(materialized='table') }}

WITH trips_data AS (
    SELECT * FROM {{ ref('stg_trips') }}
)

SELECT
    -- Grouping by the first day of the month
    DATE_TRUNC('month', pickup_datetime)::DATE AS trip_month,

    taxi_color,

    -- Aggregated metrics
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_trip_distance,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS average_trip_distance,
    AVG(total_amount) AS average_trip_fare

FROM trips_data
WHERE pickup_datetime IS NOT NULL
GROUP BY
    trip_month,
    taxi_color
ORDER BY
    trip_month,
    taxi_color
