/* @bruin
name: analytics.fct_flights
type: bq.sql
connection: gcp-default

depends:
  - staging.stg_flights
  - staging.stg_airlines
  - staging.stg_airports
  
materialization:
  type: table
  strategy: create+replace
  partition_by: flight_date
  cluster_by: ["airline_name", "origin_airport_name"]
@bruin */

WITH flights AS (
    SELECT * FROM `kestra-sandbox-486404.staging.stg_flights`
),
airlines AS (
    SELECT * FROM `kestra-sandbox-486404.staging.stg_airlines`
),
airports AS (
    SELECT * FROM `kestra-sandbox-486404.staging.stg_airports`
)

SELECT 
    f.flight_date,
    -- Новые колонки в формате DATETIME
    f.scheduled_departure,
    f.departure_time,

    -- Расчет сезона
    CASE 
        WHEN EXTRACT(MONTH FROM f.flight_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM f.flight_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM f.flight_date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM f.flight_date) IN (9, 10, 11) THEN 'Autumn'
    END as season,
    
    a.airline_name,
    f.flight_number,
    f.tail_number,
    
    -- Детали аэропортов (Origin)
    orig.airport_name as origin_airport_name,
    orig.city as origin_city,
    orig.state as origin_state,
    
    -- Детали аэропортов (Destination)
    dest.airport_name as destination_airport_name,
    dest.city as destination_city,
    
    -- Метрики полета
    f.departure_delay,
    f.arrival_delay,
    f.air_time,
    f.distance,
    f.is_cancelled,
    
    -- Детализация задержек (4 основные колонки без группировки)
    f.airline_delay,
    f.weather_delay,
    f.air_system_delay,
    f.late_aircraft_delay,
    f.security_delay -- Добавил пятой для полной точности данных

FROM flights f
LEFT JOIN airlines a ON f.airline_id = a.airline_id
LEFT JOIN airports orig ON f.origin_airport = orig.airport_id
LEFT JOIN airports dest ON f.destination_airport = dest.airport_id