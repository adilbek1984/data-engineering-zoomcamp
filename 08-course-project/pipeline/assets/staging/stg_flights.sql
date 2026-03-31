/* @bruin
name: staging.stg_flights
type: bq.sql
connection: gcp-default

materialization:
  type: table
  strategy: create+replace
  partition_by: flight_date
  cluster_by: ["airline_id", "origin_airport"]
@bruin */

WITH raw_prep AS (
    SELECT 
        SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(CAST(YEAR AS STRING), '-', CAST(MONTH AS STRING), '-', CAST(DAY AS STRING))) as f_date,
        *
    FROM `kestra-sandbox-486404.staging.ext_flights`
    WHERE YEAR IS NOT NULL AND MONTH IS NOT NULL AND DAY IS NOT NULL
),

time_logic AS (
    SELECT 
        *,
        -- Обработка часов (если 24, то 00)
        CASE 
            WHEN DIV(SCHEDULED_DEPARTURE, 100) >= 24 THEN '00'
            ELSE LPAD(CAST(DIV(SCHEDULED_DEPARTURE, 100) AS STRING), 2, '0') 
        END as sched_h,
        LPAD(CAST(MOD(SCHEDULED_DEPARTURE, 100) AS STRING), 2, '0') as sched_m,
        
        CASE 
            WHEN DIV(DEPARTURE_TIME, 100) >= 24 THEN '00'
            ELSE LPAD(CAST(DIV(DEPARTURE_TIME, 100) AS STRING), 2, '0') 
        END as act_h,
        LPAD(CAST(MOD(DEPARTURE_TIME, 100) AS STRING), 2, '0') as act_m
    FROM raw_prep
)

SELECT 
    f_date as flight_date,
    
    -- 1. Формат DATETIME (исправленный синтаксис SAFE_CAST)
    SAFE_CAST(CONCAT(CAST(f_date AS STRING), ' ', sched_h, ':', sched_m, ':00') AS DATETIME) as scheduled_departure,
    SAFE_CAST(CONCAT(CAST(f_date AS STRING), ' ', act_h, ':', act_m, ':00') AS DATETIME) as departure_time,

    -- 2. Основные ID
    AIRLINE as airline_id,
    CAST(FLIGHT_NUMBER AS STRING) as flight_number,
    TAIL_NUMBER as tail_number,
    ORIGIN_AIRPORT as origin_airport,
    DESTINATION_AIRPORT as destination_airport,
    
    -- 3. Операционные метрики (Вернул все поля сюда)
    CAST(DEPARTURE_DELAY AS INT64) as departure_delay,
    CAST(TAXI_OUT AS INT64) as taxi_out,
    CAST(WHEELS_OFF AS INT64) as wheels_off,
    CAST(SCHEDULED_TIME AS INT64) as scheduled_time,
    CAST(ELAPSED_TIME AS INT64) as elapsed_time,
    CAST(AIR_TIME AS INT64) as air_time,
    CAST(DISTANCE AS INT64) as distance,
    CAST(WHEELS_ON AS INT64) as wheels_on,
    CAST(TAXI_IN AS INT64) as taxi_in,
    
    -- 4. Задержки и статусы
    CAST(ARRIVAL_DELAY AS INT64) as arrival_delay,
    DIVERTED as is_diverted,
    CANCELLED as is_cancelled,
    CANCELLATION_REASON as cancellation_reason,
    
    -- Детализация задержек (COALESCE)
    COALESCE(AIR_SYSTEM_DELAY, 0) as air_system_delay,
    COALESCE(SECURITY_DELAY, 0) as security_delay,
    COALESCE(AIRLINE_DELAY, 0) as airline_delay,
    COALESCE(LATE_AIRCRAFT_DELAY, 0) as late_aircraft_delay,
    COALESCE(WEATHER_DELAY, 0) as weather_delay

FROM time_logic