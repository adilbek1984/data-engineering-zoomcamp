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

SELECT 
    -- 1. Создание TIMESTAMP/DATE из YEAR, MONTH, DAY
    -- Используем SAFE.PARSE_DATE на случай, если в данных попадется "31 февраля"
    SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(CAST(YEAR AS STRING), '-', CAST(MONTH AS STRING), '-', CAST(DAY AS STRING))) as flight_date,
    
    -- 2. Основные ID в нижнем регистре
    --LOWER(AIRLINE) as airline_id,
    AIRLINE as airline_id,
    CAST(FLIGHT_NUMBER AS STRING) as flight_number, -- Номер рейса лучше хранить как строку
    TAIL_NUMBER as tail_number,
    ORIGIN_AIRPORT as origin_airport,
    DESTINATION_AIRPORT as destination_airport,
    
    -- 3. Время (формат HHMM в CSV обычно сложен, пока оставляем как INT для расчетов)
    SCHEDULED_DEPARTURE as scheduled_departure,
    DEPARTURE_TIME as departure_time,
    DEPARTURE_DELAY as departure_delay,
    TAXI_OUT as taxi_out,
    WHEELS_OFF as wheels_off,
    SCHEDULED_TIME as scheduled_time,
    ELAPSED_TIME as elapsed_time,
    AIR_TIME as air_time,
    DISTANCE as distance,
    WHEELS_ON as wheels_on,
    TAXI_IN as taxi_in,
    
    -- 4. Задержки и причины (важно для аналитики)
    ARRIVAL_DELAY as arrival_delay,
    DIVERTED as is_diverted,
    CANCELLED as is_cancelled,
    CANCELLATION_REASON as cancellation_reason,
    
    -- Детализация задержек (заполняем 0, если NULL)
    COALESCE(AIR_SYSTEM_DELAY, 0) as air_system_delay,
    COALESCE(SECURITY_DELAY, 0) as security_delay,
    COALESCE(AIRLINE_DELAY, 0) as airline_delay,
    COALESCE(LATE_AIRCRAFT_DELAY, 0) as late_aircraft_delay,
    COALESCE(WEATHER_DELAY, 0) as weather_delay

FROM `kestra-sandbox-486404.staging.ext_flights`
-- Фильтруем пустые строки, чтобы не сломать PARSE_DATE
WHERE YEAR IS NOT NULL 
  AND MONTH IS NOT NULL 
  AND DAY IS NOT NULL