/* 
==================================================================
Description:
    this scripts creates the basic infrastructure for ETL project,
    including the database, schemas and warehouse
==================================================================
*/

-- set the role to SYSADMIN for object creation
USE ROLE SYSADMIN;

-- create the main database for the project
CREATE DATABASE IF NOT EXISTS TAXI_DATA_PROJECT;

-- use  the newly created database
USE DATABASE TAXI_DATA_PROJECT;

-- create schema for each layer
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- create or replace the warehouse for processing
CREATE OR REPLACE WAREHOUSE COMPUTE_WH
 WAREHOUSE_SIZE = 'XSMALL'
 AUTO_SUSPEND = 60
 AUTO_RESUME = TRUE
 INITIALLY_SUSPENDED = TRUE;

-- grant usage permissions on the warehouse to the SYSADMIN role
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

