-- Active: 1754079631037@@127.0.0.1@5432@gtfs_batch
-- -- create extension
-- CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- -- Link Server to staging database
-- CREATE SERVER gtfs_staging_server
-- FOREIGN DATA WRAPPER postgres_fdw
-- OPTIONS (host 'postgres', dbname 'gtfs_batch_staging', port '5432');

-- -- Link the User to Server
-- CREATE USER MAPPING FOR admin
-- SERVER gtfs_staging_server
-- OPTIONS (user 'admin', password 'password');

-- Drop all foreign tables if they exist
DROP FOREIGN TABLE IF EXISTS stg_trips;
DROP FOREIGN TABLE IF EXISTS stg_agency;
DROP FOREIGN TABLE IF EXISTS stg_calendar;
DROP FOREIGN TABLE IF EXISTS stg_calendar_dates;
DROP FOREIGN TABLE IF EXISTS stg_routes;
DROP FOREIGN TABLE IF EXISTS stg_shapes;
DROP FOREIGN TABLE IF EXISTS stg_stops;
DROP FOREIGN TABLE IF EXISTS stg_stop_times;

-- Import all needed tables from staging database
IMPORT FOREIGN SCHEMA public
LIMIT TO (
    stg_trips,
    stg_agency,
    stg_calendar,
    stg_calendar_dates,
    stg_routes,
    stg_shapes,
    stg_stops,
    stg_stop_times
)
FROM SERVER gtfs_staging_server
INTO public;

