-- Active: 1754079683948@@127.0.0.1@8123@gtfs_batch
-- Active: 1754079683948@@127.0.0.1@8123@gtfs_batch: 1754079631037@@127.0.0.1@5432@gtfs_batch: 1754079631037@@127.0.0.1@5432@gtfs_batch: 1754079683948@@127.0.0.1@8123@gtfs_batch: 1754079631037@@127.0.0.1@5432@gtfs_batch_staging: 1754079631037@@127.0.0.1@5432@gtfs_batch: 1754079631037@@127.0.0.1@5432@gtfs_batch
USE gtfs_batch;

---------------------
-- agency
---------------------
CREATE TABLE gtfs_batch.agency_clean
ENGINE = MergeTree
ORDER BY (agency_id, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY agency_id
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.agency
)
SELECT agency_id, agency_name, agency_url, agency_timezone,
       agency_lang, agency_phone,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.agency;
RENAME TABLE gtfs_batch.agency_clean TO gtfs_batch.agency;

---------------------
-- calendar
---------------------
CREATE TABLE gtfs_batch.calendar_clean
ENGINE = MergeTree
ORDER BY (service_id, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY service_id
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.calendar
)
SELECT service_id, monday, tuesday, wednesday, thursday,
       friday, saturday, sunday, start_date, end_date,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.calendar;
RENAME TABLE gtfs_batch.calendar_clean TO gtfs_batch.calendar;

---------------------
-- calendar_dates
---------------------
CREATE TABLE gtfs_batch.calendar_dates_clean
ENGINE = MergeTree
ORDER BY (service_id, date, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY service_id, date
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.calendar_dates
)
SELECT service_id, date, exception_type,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.calendar_dates;
RENAME TABLE gtfs_batch.calendar_dates_clean TO gtfs_batch.calendar_dates;

---------------------
-- routes
---------------------
CREATE TABLE gtfs_batch.routes_clean
ENGINE = MergeTree
ORDER BY (route_id, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY route_id
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.routes
)
SELECT route_id, agency_id, route_short_name, route_long_name,
       route_desc, route_type, route_color, route_text_color,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.routes;
RENAME TABLE gtfs_batch.routes_clean TO gtfs_batch.routes;

---------------------
-- shapes
---------------------
CREATE TABLE gtfs_batch.shapes_clean
ENGINE = MergeTree
ORDER BY (shape_id, shape_pt_sequence, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY shape_id, shape_pt_sequence
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.shapes
)
SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.shapes;
RENAME TABLE gtfs_batch.shapes_clean TO gtfs_batch.shapes;

---------------------
-- stops
---------------------
CREATE TABLE gtfs_batch.stops_clean
ENGINE = MergeTree
ORDER BY (stop_id, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY stop_id
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.stops
)
SELECT stop_id, stop_name, stop_desc, stop_lat, stop_lon,
       zone_id, stop_url, location_type, parent_station,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.stops;
RENAME TABLE gtfs_batch.stops_clean TO gtfs_batch.stops;

---------------------
-- stop_times
---------------------
CREATE TABLE gtfs_batch.stop_times_clean
ENGINE = ReplacingMergeTree
ORDER BY (trip_id, stop_sequence, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY trip_id, stop_sequence
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.stop_times
)
SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence,
       pickup_type, drop_off_type, timepoint,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.stop_times;
RENAME TABLE gtfs_batch.stop_times_clean TO gtfs_batch.stop_times;

---------------------
-- trips
---------------------
CREATE TABLE gtfs_batch.trips_clean
ENGINE = MergeTree
ORDER BY (trip_id, start_dt_scd) AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY trip_id
               ORDER BY start_dt_scd DESC
           ) AS rn
    FROM gtfs_batch.trips
)
SELECT route_id, service_id, trip_id, trip_headsign, direction_id,
       block_id, shape_id,
       start_dt_scd, end_dt_scd, is_current
FROM ranked
WHERE rn = 1;

DROP TABLE gtfs_batch.trips;
RENAME TABLE gtfs_batch.trips_clean TO gtfs_batch.trips;
