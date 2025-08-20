-- =============================================================================
-- 03_load_full_data.sql
--
-- Description:
-- This script performs the full data load for both yellow and green taxi
-- trips from 2014 to 2025. It temporarily scales up the warehouse for
-- performance.
-- =============================================================================

-- Set the context
USE ROLE SYSADMIN;
USE DATABASE TAXI_DATA_PROJECT;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- Step 1: Scale up warehouse for performance
-- -----------------------------------------------------------------------------
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE' WAIT_FOR_COMPLETION = TRUE;


-- -----------------------------------------------------------------------------
-- Step 2: Load all Yellow Taxi data (2014-2025)
-- -----------------------------------------------------------------------------
-- Truncate the table to ensure a full reload
TRUNCATE TABLE IF EXISTS RAW_YELLOW_TRIPS;

COPY INTO RAW_YELLOW_TRIPS(RAW_RECORD)
FROM (
    SELECT $1
    FROM @YELLOW_TRIP_STAGE
)
-- Use a regex pattern to match all yellow taxi files from 2014 to 2025
PATTERN = '.*yellow_tripdata_20(1[4-9]|2[0-5])-.*\\.parquet'
ON_ERROR = 'CONTINUE';


-- -----------------------------------------------------------------------------
-- Step 3: Load all Green Taxi data (2014-2025)
-- -----------------------------------------------------------------------------
-- Truncate the table to ensure a full reload
TRUNCATE TABLE IF EXISTS RAW_GREEN_TRIPS;

COPY INTO RAW_GREEN_TRIPS(RAW_RECORD)
FROM (
    SELECT $1
    FROM @GREEN_TRIP_STAGE
)
-- Use a regex pattern to match all green taxi files from 2014 to 2025
PATTERN = '.*green_tripdata_20(1[4-9]|2[0-5])-.*\\.parquet'
ON_ERROR = 'CONTINUE';


-- -----------------------------------------------------------------------------
-- NOTE: DO NOT SCALE DOWN THE WAREHOUSE YET.
-- We will do this in the final step after the dbt build is complete.
-- -----------------------------------------------------------------------------
