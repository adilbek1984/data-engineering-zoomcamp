/* @bruin
name: reports.winter_delay_analysis
type: bq.sql
connection: gcp-default

materialization:
  type: table
  strategy: create+replace

depends:
  - analytics.fct_flights
@bruin */

SELECT 
    origin_airport_name,
    origin_state,
    airline_name,
    -- Считаем количество рейсов
    COUNT(*) as total_winter_flights,
    -- Средняя задержка прилета
    ROUND(AVG(arrival_delay), 2) as avg_arrival_delay,
    -- Суммарная задержка из-за погоды в минутах
    SUM(external_caused_delay) as total_weather_system_delay,
    -- Процент рейсов с задержкой более 30 минут
    ROUND(COUNTIF(arrival_delay > 30) / COUNT(*) * 100, 2) as heavy_delay_probability
FROM `kestra-sandbox-486404.analytics.fct_flights`
WHERE season = 'Winter'
  AND is_cancelled = 0 -- Берем только те, что вылетели
GROUP BY 1, 2, 3
HAVING total_winter_flights > 100 -- Убираем редкие рейсы для чистоты статистики
ORDER BY avg_arrival_delay DESC
LIMIT 20