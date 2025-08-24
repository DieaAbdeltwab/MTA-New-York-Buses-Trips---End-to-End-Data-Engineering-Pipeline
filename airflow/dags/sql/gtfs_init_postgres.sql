-- Active: 1754079631037@@127.0.0.1@5432@gtfs_batch

-- agency table
CREATE TABLE IF NOT EXISTS agency (
    agency_id TEXT,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- calendar table
CREATE TABLE IF NOT EXISTS calendar (
    service_id TEXT,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date DATE,
    end_date DATE,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- calendar_dates table
CREATE TABLE IF NOT EXISTS calendar_dates (
    service_id TEXT,
    date DATE,
    exception_type INTEGER,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- routes table
CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- shapes table
CREATE TABLE IF NOT EXISTS shapes (
    shape_id TEXT,
    shape_pt_lat DOUBLE PRECISION,
    shape_pt_lon DOUBLE PRECISION,
    shape_pt_sequence INTEGER,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- stops table
CREATE TABLE IF NOT EXISTS stops (
    stop_id INTEGER,
    stop_name TEXT,
    stop_desc TEXT,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- stop_times table
CREATE TABLE IF NOT EXISTS stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id INTEGER,
    stop_sequence INTEGER,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    timepoint INTEGER,
    -- SCD2 columns
    start_dt_scd TIMESTAMP WITH TIME ZONE,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- trips table
CREATE TABLE IF NOT EXISTS trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER,
    block_id INTEGER,
    shape_id TEXT,
    -- SCD2 columns
    start_dt_scd TIMESTAMP,
    end_dt_scd TIMESTAMP,
    is_current BOOLEAN
);

-- create extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Link Server to staging database
CREATE SERVER gtfs_staging_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'postgres', dbname 'gtfs_batch_staging', port '5432');

-- Link the User to Server
CREATE USER MAPPING FOR admin
SERVER gtfs_staging_server
OPTIONS (user 'admin', password 'password');