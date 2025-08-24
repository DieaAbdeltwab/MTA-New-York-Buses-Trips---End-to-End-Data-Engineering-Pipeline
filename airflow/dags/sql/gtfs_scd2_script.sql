-- Active: 1754079631037@@127.0.0.1@5432@gtfs_batch

--------------------- sd2_trips ---------------------
-- 1) Close existing current records that have changed
UPDATE trips t
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_trips s
WHERE t.trip_id = s.trip_id
  AND t.is_current = true
  AND (
         t.route_id       IS DISTINCT FROM s.route_id
      OR t.service_id     IS DISTINCT FROM s.service_id
      OR t.trip_headsign  IS DISTINCT FROM s.trip_headsign
      OR t.direction_id   IS DISTINCT FROM s.direction_id
      OR t.block_id       IS DISTINCT FROM s.block_id
      OR t.shape_id       IS DISTINCT FROM s.shape_id
  );

-- 2) Insert brand new trips or changed ones (new version)
INSERT INTO trips (
    trip_id, route_id, service_id, trip_headsign, direction_id, block_id, shape_id,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.trip_id,
    s.route_id,
    s.service_id,
    s.trip_headsign,
    s.direction_id,
    s.block_id,
    s.shape_id,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_trips s
LEFT JOIN trips t
  ON t.trip_id = s.trip_id AND t.is_current = true
WHERE t.trip_id IS NULL
   OR (
         t.route_id       IS DISTINCT FROM s.route_id
      OR t.service_id     IS DISTINCT FROM s.service_id
      OR t.trip_headsign  IS DISTINCT FROM s.trip_headsign
      OR t.direction_id   IS DISTINCT FROM s.direction_id
      OR t.block_id       IS DISTINCT FROM s.block_id
      OR t.shape_id       IS DISTINCT FROM s.shape_id
   );


--------------------- scd2_agency ---------------------
-- 1) Close existing current records that have changed
UPDATE agency a
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_agency s
WHERE a.agency_id = s.agency_id
  AND a.is_current = true
  AND (
         a.agency_name     IS DISTINCT FROM s.agency_name
      OR a.agency_url      IS DISTINCT FROM s.agency_url
      OR a.agency_timezone IS DISTINCT FROM s.agency_timezone
      OR a.agency_lang     IS DISTINCT FROM s.agency_lang
      OR a.agency_phone    IS DISTINCT FROM s.agency_phone
  );

-- 2) Insert brand new agencies or changed ones (new version)
INSERT INTO agency (
    agency_id, agency_name, agency_url, agency_timezone, agency_lang, agency_phone,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.agency_id,
    s.agency_name,
    s.agency_url,
    s.agency_timezone,
    s.agency_lang,
    s.agency_phone,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_agency s
LEFT JOIN agency a
  ON a.agency_id = s.agency_id AND a.is_current = true
WHERE a.agency_id IS NULL
   OR (
         a.agency_name     IS DISTINCT FROM s.agency_name
      OR a.agency_url      IS DISTINCT FROM s.agency_url
      OR a.agency_timezone IS DISTINCT FROM s.agency_timezone
      OR a.agency_lang     IS DISTINCT FROM s.agency_lang
      OR a.agency_phone    IS DISTINCT FROM s.agency_phone
   );

--------------------- scd2_calendar ---------------------
-- 1) Close existing current records that have changed
UPDATE calendar c
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_calendar s
WHERE c.service_id = s.service_id
  AND c.is_current = true
  AND (
         c.monday     IS DISTINCT FROM s.monday
      OR c.tuesday    IS DISTINCT FROM s.tuesday
      OR c.wednesday  IS DISTINCT FROM s.wednesday
      OR c.thursday   IS DISTINCT FROM s.thursday
      OR c.friday     IS DISTINCT FROM s.friday
      OR c.saturday   IS DISTINCT FROM s.saturday
      OR c.sunday     IS DISTINCT FROM s.sunday
      OR c.start_date IS DISTINCT FROM s.start_date
      OR c.end_date   IS DISTINCT FROM s.end_date
  );

-- 2) Insert brand new calendar rows or changed ones (new version)
INSERT INTO calendar (
    service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
    start_date, end_date, start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.service_id,
    s.monday,
    s.tuesday,
    s.wednesday,
    s.thursday,
    s.friday,
    s.saturday,
    s.sunday,
    s.start_date,
    s.end_date,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_calendar s
LEFT JOIN calendar c
  ON c.service_id = s.service_id
 AND c.is_current = true
WHERE c.service_id IS NULL
   OR (
         c.monday     IS DISTINCT FROM s.monday
      OR c.tuesday    IS DISTINCT FROM s.tuesday
      OR c.wednesday  IS DISTINCT FROM s.wednesday
      OR c.thursday   IS DISTINCT FROM s.thursday
      OR c.friday     IS DISTINCT FROM s.friday
      OR c.saturday   IS DISTINCT FROM s.saturday
      OR c.sunday     IS DISTINCT FROM s.sunday
      OR c.start_date IS DISTINCT FROM s.start_date
      OR c.end_date   IS DISTINCT FROM s.end_date
   );


--------------------- scd2_calendar_dates ---------------------
-- 1) Close existing current records that have changed
UPDATE calendar_dates c
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_calendar_dates s
WHERE c.service_id = s.service_id
  AND c.date = s.date
  AND c.is_current = true
  AND (
         c.exception_type IS DISTINCT FROM s.exception_type
  );

-- 2) Insert brand new calendar_dates records or changed ones (new version)
INSERT INTO calendar_dates (
    service_id, date, exception_type,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.service_id,
    s.date,
    s.exception_type,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_calendar_dates s
LEFT JOIN calendar_dates c
  ON c.service_id = s.service_id
 AND c.date = s.date
 AND c.is_current = true
WHERE c.service_id IS NULL
   OR c.date IS NULL
   OR (
         c.exception_type IS DISTINCT FROM s.exception_type
   );

--------------------- scd2_routes ---------------------
-- 1) Close existing current records that have changed
UPDATE routes r
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_routes s
WHERE r.route_id = s.route_id
  AND r.is_current = true
  AND (
         r.agency_id         IS DISTINCT FROM s.agency_id
      OR r.route_short_name  IS DISTINCT FROM s.route_short_name
      OR r.route_long_name   IS DISTINCT FROM s.route_long_name
      OR r.route_desc        IS DISTINCT FROM s.route_desc
      OR r.route_type        IS DISTINCT FROM s.route_type
      OR r.route_color       IS DISTINCT FROM s.route_color
      OR r.route_text_color  IS DISTINCT FROM s.route_text_color
  );

-- 2) Insert brand new routes or changed ones (new version)
INSERT INTO routes (
    route_id, agency_id, route_short_name, route_long_name, route_desc,
    route_type, route_color, route_text_color,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.route_id,
    s.agency_id,
    s.route_short_name,
    s.route_long_name,
    s.route_desc,
    s.route_type,
    s.route_color,
    s.route_text_color,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_routes s
LEFT JOIN routes r
  ON r.route_id = s.route_id
 AND r.is_current = true
WHERE r.route_id IS NULL
   OR (
         r.agency_id         IS DISTINCT FROM s.agency_id
      OR r.route_short_name  IS DISTINCT FROM s.route_short_name
      OR r.route_long_name   IS DISTINCT FROM s.route_long_name
      OR r.route_desc        IS DISTINCT FROM s.route_desc
      OR r.route_type        IS DISTINCT FROM s.route_type
      OR r.route_color       IS DISTINCT FROM s.route_color
      OR r.route_text_color  IS DISTINCT FROM s.route_text_color
   );

--------------------- scd2_shapes ---------------------
-- 1) Close existing current records that have changed
UPDATE shapes sh
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_shapes s
WHERE sh.shape_id = s.shape_id
  AND sh.shape_pt_sequence = s.shape_pt_sequence
  AND sh.is_current = true
  AND (
         sh.shape_pt_lat IS DISTINCT FROM s.shape_pt_lat
      OR sh.shape_pt_lon IS DISTINCT FROM s.shape_pt_lon
  );

-- 2) Insert brand new shapes or changed ones (new version)
INSERT INTO shapes (
    shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.shape_id,
    s.shape_pt_lat,
    s.shape_pt_lon,
    s.shape_pt_sequence,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_shapes s
LEFT JOIN shapes sh
  ON sh.shape_id = s.shape_id
 AND sh.shape_pt_sequence = s.shape_pt_sequence
 AND sh.is_current = true
WHERE sh.shape_id IS NULL
   OR (
         sh.shape_pt_lat IS DISTINCT FROM s.shape_pt_lat
      OR sh.shape_pt_lon IS DISTINCT FROM s.shape_pt_lon
   );

--------------------- scd2_stop_times ---------------------
-- 1) Close existing current records that have changed
UPDATE stop_times st
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_stop_times s
WHERE st.trip_id = s.trip_id
  AND st.stop_sequence = s.stop_sequence
  AND st.is_current = true
  AND (
         st.arrival_time   IS DISTINCT FROM s.arrival_time
      OR st.departure_time IS DISTINCT FROM s.departure_time
      OR st.stop_id        IS DISTINCT FROM s.stop_id
      OR st.pickup_type    IS DISTINCT FROM s.pickup_type
      OR st.drop_off_type  IS DISTINCT FROM s.drop_off_type
      OR st.timepoint      IS DISTINCT FROM s.timepoint
  );

-- 2) Insert brand new stop_times or changed ones (new version)
INSERT INTO stop_times (
    trip_id, arrival_time, departure_time, stop_id, stop_sequence,
    pickup_type, drop_off_type, timepoint,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.trip_id,
    s.arrival_time,
    s.departure_time,
    s.stop_id,
    s.stop_sequence,
    s.pickup_type,
    s.drop_off_type,
    s.timepoint,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_stop_times s
LEFT JOIN stop_times st
  ON st.trip_id = s.trip_id
 AND st.stop_sequence = s.stop_sequence
 AND st.is_current = true
WHERE st.trip_id IS NULL
   OR (
         st.arrival_time   IS DISTINCT FROM s.arrival_time
      OR st.departure_time IS DISTINCT FROM s.departure_time
      OR st.stop_id        IS DISTINCT FROM s.stop_id
      OR st.pickup_type    IS DISTINCT FROM s.pickup_type
      OR st.drop_off_type  IS DISTINCT FROM s.drop_off_type
      OR st.timepoint      IS DISTINCT FROM s.timepoint
   );

--------------------- scd2_stops ---------------------
-- 1) Close existing current records that have changed
UPDATE stops st
SET is_current = false,
    end_dt_scd = s.start_dt_scd
FROM stg_stops s
WHERE st.stop_id = s.stop_id
  AND st.is_current = true
  AND (
         st.stop_name       IS DISTINCT FROM s.stop_name
      OR st.stop_desc       IS DISTINCT FROM s.stop_desc
      OR st.stop_lat        IS DISTINCT FROM s.stop_lat
      OR st.stop_lon        IS DISTINCT FROM s.stop_lon
      OR st.zone_id         IS DISTINCT FROM s.zone_id
      OR st.stop_url        IS DISTINCT FROM s.stop_url
      OR st.location_type   IS DISTINCT FROM s.location_type
      OR st.parent_station  IS DISTINCT FROM s.parent_station
  );

-- 2) Insert brand new stops or changed ones (new version)
INSERT INTO stops (
    stop_id, stop_name, stop_desc, stop_lat, stop_lon,
    zone_id, stop_url, location_type, parent_station,
    start_dt_scd, end_dt_scd, is_current
)
SELECT
    s.stop_id,
    s.stop_name,
    s.stop_desc,
    s.stop_lat,
    s.stop_lon,
    s.zone_id,
    s.stop_url,
    s.location_type,
    s.parent_station,
    s.start_dt_scd,
    NULL AS end_dt_scd,
    TRUE AS is_current
FROM stg_stops s
LEFT JOIN stops st
  ON st.stop_id = s.stop_id
 AND st.is_current = true
WHERE st.stop_id IS NULL
   OR (
         st.stop_name       IS DISTINCT FROM s.stop_name
      OR st.stop_desc       IS DISTINCT FROM s.stop_desc
      OR st.stop_lat        IS DISTINCT FROM s.stop_lat
      OR st.stop_lon        IS DISTINCT FROM s.stop_lon
      OR st.zone_id         IS DISTINCT FROM s.zone_id
      OR st.stop_url        IS DISTINCT FROM s.stop_url
      OR st.location_type   IS DISTINCT FROM s.location_type
      OR st.parent_station  IS DISTINCT FROM s.parent_station
   );
