-- [MASTER] elt_run.sql
-- Description: Master orchestration script for Pramana DWH Migration.
-- Note: Run this script with a superuser connection to initialize the DB and schemas.

--------------------------------------------------------------------------------
-- 1. INITIALIZATION (RUN THESE OUTSIDE TRANSACTIONS IF NECESSARY)
--------------------------------------------------------------------------------
-- CREATE DATABASE pramana_v4_db; 

-- Connect to pramana_v4_db
-- \c pramana_v4_db

CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS staging; -- For intermediate processing if needed
CREATE SCHEMA IF NOT EXISTS dw;

--------------------------------------------------------------------------------
-- 2. SCHEMA DEFINITIONS (DDL)
--------------------------------------------------------------------------------
-- Step A: Initialize source tables
\i source_schema_change_v4.sql

-- Step B: Initialize Data Warehouse tables (Dimensions & Facts)
\i dw_schema_change_v4.sql

--------------------------------------------------------------------------------
-- 3. DATA LOADING (DML - ELT)
--------------------------------------------------------------------------------
BEGIN;

-- Step C: Load Dimensions (SCD Type 1)
-- Logic includes Date Dim generation and Master data joins.
\i elt_dim.sql

-- Step D: Load Fact Tables
-- Joins source tables with dimensions for surrogate keys.
\i elt_fact.sql

-- Step E: Precompute Aggregations
-- Rollups for dashboard performance.
\i elt_agg.sql

COMMIT;

--------------------------------------------------------------------------------
-- 4. FINAL VERIFICATION
--------------------------------------------------------------------------------
-- Optional: Record counts for audit
SELECT 'Source Users' as table, count(*) from source.mst_user
UNION ALL
SELECT 'DW Users' as table, count(*) from dw.dim_user
UNION ALL
SELECT 'DW Sessions' as table, count(*) from dw.fact_session;
