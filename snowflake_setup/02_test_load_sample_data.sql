-- =============================================================================
-- 02_test_load_sample_data.sql
--
-- Description:
-- This script loads a single sample file from our S3 bucket into the
-- raw table to test the storage integration.
-- =============================================================================

-- Set the context
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE TAXI_DATA_PROJECT;
USE SCHEMA RAW;

-- Truncate the table to ensure a clean load
TRUNCATE TABLE IF EXISTS RAW_YELLOW_TRIPS;

-- Load data from the S3 stage using the storage integration
-- We now point our stage to our own bucket and use the integration
CREATE OR REPLACE STAGE YELLOW_TRIP_STAGE_INTEGRATED
    STORAGE_INTEGRATION = s3_integration -- Use the integration object
    URL = 's3://te-nyc-taxi-snowflake-elt-project/' -- Use your bucket URL
    FILE_FORMAT = PARQUET_FILE_FORMAT;

-- Use COPY INTO to load the specific sample file
COPY INTO RAW_YELLOW_TRIPS(RAW_RECORD)
FROM (
    SELECT $1
    FROM @YELLOW_TRIP_STAGE_INTEGRATED/yellow_tripdata_2024-01.parquet -- Specify the file path
)
ON_ERROR = 'SKIP_FILE';

-- Verify the load
SELECT * FROM RAW_YELLOW_TRIPS LIMIT 10;