-- Active: 1751788548434@@127.0.0.1@8123@default

CREATE DATABASE IF NOT EXISTS gtfs_batch;
USE gtfs_batch;

CREATE TABLE IF NOT EXISTS gtfs_batch.agency (
    agency_id String,
    agency_name String,
    agency_url String,
    agency_timezone String,
    agency_lang Nullable(String),
    agency_phone Nullable(String)
) ENGINE = MergeTree
ORDER BY agency_id;

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
    end_date Date
) ENGINE = MergeTree
ORDER BY service_id;

CREATE TABLE IF NOT EXISTS gtfs_batch.calendar_dates (
    service_id String,
    date Date,
    exception_type Nullable(UInt8)
) ENGINE = MergeTree
ORDER BY (service_id, date);

CREATE TABLE IF NOT EXISTS gtfs_batch.routes (
    route_id String,
    agency_id String,
    route_short_name String,
    route_long_name String,
    route_desc Nullable(String),
    route_type Nullable(Int32),
    route_color Nullable(String),
    route_text_color Nullable(String)
) ENGINE = MergeTree
ORDER BY route_id;

CREATE TABLE IF NOT EXISTS gtfs_batch.shapes (
    shape_id String,
    shape_pt_lat Float64,
    shape_pt_lon Float64,
    shape_pt_sequence UInt32
) ENGINE = MergeTree
ORDER BY (shape_id, shape_pt_sequence);

CREATE TABLE IF NOT EXISTS gtfs_batch.stops (
    stop_id String,
    stop_name String,
    stop_desc Nullable(String),
    stop_lat Float64,
    stop_lon Float64,
    zone_id Nullable(String),
    stop_url Nullable(String),
    location_type Nullable(UInt8),
    parent_station Nullable(String)
) ENGINE = MergeTree
ORDER BY stop_id;

CREATE TABLE IF NOT EXISTS gtfs_batch.stop_times (
    trip_id String,
    arrival_time String,
    departure_time String,
    stop_id String,
    stop_sequence UInt32,
    pickup_type Nullable(UInt8),
    drop_off_type Nullable(UInt8),
    timepoint Nullable(UInt8)
) ENGINE = ReplacingMergeTree
ORDER BY (trip_id, stop_sequence);

CREATE TABLE IF NOT EXISTS gtfs_batch.trips (
    route_id String,
    service_id String,
    trip_id String,
    trip_headsign String,
    direction_id Nullable(UInt8),
    block_id Nullable(String),
    shape_id Nullable(String)
) ENGINE = MergeTree
ORDER BY trip_id;
