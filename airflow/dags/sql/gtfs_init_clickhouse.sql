-- Active: 1754079683948@@127.0.0.1@8123@system
CREATE DATABASE IF NOT EXISTS gtfs_batch;
USE gtfs_batch;

CREATE TABLE IF NOT EXISTS gtfs_batch.agency (
    agency_id String,
    agency_name String,
    agency_url String,
    agency_timezone String,
    agency_lang Nullable(String),
    agency_phone Nullable(String),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (agency_id, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.calendar (
    service_id String,
    monday Nullable(UInt8),
    tuesday Nullable(UInt8),
    wednesday Nullable(UInt8),
    thursday Nullable(UInt8),
    friday Nullable(UInt8),
    saturday Nullable(UInt8),
    sunday Nullable(UInt8),
    start_date Date,
    end_date Date,
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (service_id, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.calendar_dates (
    service_id String,
    date Date,
    exception_type Nullable(UInt8),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (service_id, date, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.routes (
    route_id String,
    agency_id String,
    route_short_name String,
    route_long_name String,
    route_desc Nullable(String),
    route_type Nullable(Int32),
    route_color Nullable(String),
    route_text_color Nullable(String),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (route_id, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.shapes (
    shape_id String,
    shape_pt_lat Float64,
    shape_pt_lon Float64,
    shape_pt_sequence UInt32,
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (shape_id, shape_pt_sequence, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.stops (
    stop_id UInt32,
    stop_name String,
    stop_desc Nullable(String),
    stop_lat Float64,
    stop_lon Float64,
    zone_id Nullable(String),
    stop_url Nullable(String),
    location_type Nullable(UInt8),
    parent_station Nullable(String),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (stop_id, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.stop_times (
    trip_id String,
    arrival_time String,
    departure_time String,
    stop_id UInt32,
    stop_sequence UInt32,
    pickup_type Nullable(UInt8),
    drop_off_type Nullable(UInt8),
    timepoint Nullable(UInt8),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = ReplacingMergeTree
ORDER BY (trip_id, stop_sequence, start_dt_scd);

CREATE TABLE IF NOT EXISTS gtfs_batch.trips (
    route_id String,
    service_id String,
    trip_id String,
    trip_headsign String,
    direction_id Nullable(UInt8),
    block_id Nullable(Int32),
    shape_id Nullable(String),
    -- SCD2 columns
    start_dt_scd DateTime,
    end_dt_scd Nullable(DateTime),
    is_current Bool
) ENGINE = MergeTree
ORDER BY (trip_id, start_dt_scd);