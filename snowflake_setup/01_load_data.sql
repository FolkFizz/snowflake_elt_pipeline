/*
==========================================================
Description:
    this scripts creates the tables for raw data and 
    the external stages to connect to the public S3 bucket
==========================================================
 */ 

 -- set the context
 USE ROLE SYSADMIN;
 USE WAREHOUSE COMPUTE_WH;
 USE DATABASE TAXI_DATA_PROJECT;
 USE SCHEMA RAW;


/*
- Create Raw Tables
- Create 2 seperate tables for yellow and green taxi data.
- The core column is a VARIANT type to handle schema evolution flexibility
- A load timestamp is added as a best practice for data lineage.
 */ 

 CREATE OR REPLACE TABLE RAW_YELLOW_TRIPS (
    RAW_RECORD VARIANT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
 );

CREATE OR REPLACE TABLE RAW_GREEN_TRIPS (
    RAW_RECORD VARIANT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


/*
- Create File Format
- A single file format can be reused for loading all Parquet files.
 */ 
 CREATE OR REPLACE FILE FORMAT PARQUET_FILE_FORMAT
    TYPE = 'PARQUET';

-- -----------------------------------------------------------------------------
-- Create Stages using the Storage Integration
-- These stages point to our private S3 bucket and use the secure
-- integration object we configured.
-- -----------------------------------------------------------------------------

-- Create an external stage for Yellow Taxi data
-- This stage now uses the STORAGE_INTEGRATION we created earlier.
CREATE OR REPLACE STAGE YELLOW_TRIP_STAGE
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://te-nyc-taxi-snowflake-elt-project/' -- Use your bucket URL
    FILE_FORMAT = PARQUET_FILE_FORMAT;

-- Create an external stage for Green Taxi data
CREATE OR REPLACE STAGE GREEN_TRIP_STAGE
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://te-nyc-taxi-snowflake-elt-project/' -- Use your bucket URL
    FILE_FORMAT = PARQUET_FILE_FORMAT;