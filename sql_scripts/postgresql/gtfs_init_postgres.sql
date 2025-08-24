-- Active: 1754587709443@@127.0.0.1@5432@gtfs_batch

--CREATE DATABASE gtfs_batch;

-- CREATE DATABASE gtfs_batch_staging;


-- Select * 
-- from stg_stop_times 
-- where trip_id = 'JA_C5-Sunday-061000_Q17_304' 
-- ORDER BY stop_sequence;


-- Select *
-- from stop_times
-- limit 10;


-- 1) Close existing current records that have changed
UPDATE trips t
SET is_current = false,
    end_dt = s.load_dt
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
    start_dt, end_dt, is_current, load_dt
)
SELECT
    s.trip_id,
    s.route_id,
    s.service_id,
    s.trip_headsign,
    s.direction_id,
    s.block_id,
    s.shape_id,
    s.load_dt AS start_dt,
    NULL AS end_dt,
    TRUE AS is_current,
    s.load_dt
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

---------------------------------------------------------------------------
Select * 
from trips 
where is_current = FALSE 

UPDATE stg_trips
SET direction_id = 1
WHERE trip_id = 'EN_C5-Weekday-031300_SBS82_903'

Select * 
from stg_trips 
where trip_id = 'EN_C5-Weekday-031300_SBS82_903'

Select * 
from trips 
where trip_id = 'EN_C5-Weekday-031300_SBS82_903' --direction_id = 1


UPDATE trips
SET direction_id = 0
WHERE trip_id = 'EN_C5-Weekday-031300_SBS82_903'


----------------------------- Run FWD on stg_trips once ----------------------
-- تأكد إنك في الـ database الأساسية gtfs_batch
\c gtfs_batch;

-- إنشاء الـ extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- عمل Server Link للـ staging database
CREATE SERVER gtfs_staging_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'postgres', dbname 'gtfs_batch_staging', port '5432');

-- ربط اليوزر الحالي بالسيرفر
CREATE USER MAPPING FOR admin
SERVER gtfs_staging_server
OPTIONS (user 'admin', password 'password');

-- استيراد جدول stg_trips من قاعدة البيانات التانية
DROP FOREIGN TABLE IF EXISTS stg_trips;

IMPORT FOREIGN SCHEMA public
LIMIT TO (stg_trips)
FROM SERVER gtfs_staging_server
INTO public;

--------------------------------------------------------------------


-- 1) إضافة أعمدة جديدة مؤقتة
ALTER TABLE calendar
ADD COLUMN start_date_tmp DATE,
ADD COLUMN end_date_tmp DATE;

-- 2) تحويل القيم من Integer إلى Date
UPDATE calendar
SET start_date_tmp = TO_DATE(start_date::TEXT, 'YYYYMMDD'),
    end_date_tmp   = TO_DATE(end_date::TEXT, 'YYYYMMDD');

-- 3) حذف الأعمدة القديمة
ALTER TABLE calendar
DROP COLUMN start_date,
DROP COLUMN end_date;

-- 4) إعادة تسمية الأعمدة الجديدة
ALTER TABLE calendar
RENAME COLUMN start_date_tmp TO start_date;

ALTER TABLE calendar
RENAME COLUMN end_date_tmp TO end_date;

-------------------

-- 1) إضافة أعمدة جديدة مؤقتة
ALTER TABLE calendar_dates
ADD COLUMN date_tmp DATE;

-- 2) تحويل القيم من Integer إلى Date
UPDATE calendar_dates
SET date_tmp = TO_DATE(date::TEXT, 'YYYYMMDD');

-- 3) حذف الأعمدة القديمة
ALTER TABLE calendar_dates
DROP COLUMN date;

-- 4) إعادة تسمية الأعمدة الجديدة
ALTER TABLE calendar_dates
RENAME COLUMN date_tmp TO date;

-------------------- test scd2 airflow -----------------------
Select * 
from calendar 

UPDATE calendar
SET saturday = 0
WHERE service_id = 'GA_C5-Saturday' -- saturday = 1


Select * 
from calendar_dates

UPDATE calendar_dates
SET date = 20250714
WHERE service_id = 'JG_C5-Weekday-BM' -- date = 20250714

UPDATE calendar_dates
SET exception_type = 1
WHERE service_id = 'CS_C5-Weekday-SDon-BM' -- exception_type = 2

Select * 
from routes

UPDATE routes
SET route_type = 1
WHERE route_id = 'M57' -- route_type = 3

Select * 
from shapes

UPDATE shapes
SET shape_pt_lat = 4000.642822
WHERE shape_id = 'S400032' and  shape_pt_sequence = 10009 -- shape_pt_lat = 4000.642822

Select * 
from shapes

UPDATE stop_times
SET pickup_type = 1
WHERE trip_id = 'EN_C5-Weekday-073600_SBS82_905' and  stop_sequence = 5 -- pickup_type = 0

Select * 
from stops

UPDATE stops
SET zone_id = 1
WHERE stop_id = 300219 -- zone_id = null



Select * 
from stops 
where stop_id = 300219 -- zone_id = null

Select * 
from stop_times 
where trip_id = 'EN_C5-Weekday-073600_SBS82_905' and  stop_sequence = 5 -- pickup_type = 0

Select * 
from shapes 
where shape_id = 'S400032' and  shape_pt_sequence = 10009 -- shape_pt_lat = 40.642822


Select * 
from routes 
where route_id = 'M57' -- route_type = 3


Select * 
from calendar_dates 
where service_id = 'JG_C5-Weekday-BM'

Select * 
from calendar_dates 
where service_id = 'CS_C5-Weekday-SDon-BM'


Select * 
from calendar 
where service_id = 'GA_C5-Saturday' 


------------------------------------------

SELECT *
FROM stops
WHERE is_current = FALSE

SELECT *
FROM stops
WHERE stop_id = 302469
