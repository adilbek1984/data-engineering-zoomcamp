/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: Date of the trip derived from pickup_datetime
    primary_key: true
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: payment_type_name
    type: varchar
    description: Human-readable payment method name
    primary_key: true
  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: positive
  - name: total_passengers
    type: bigint
    description: Total number of passengers transported
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total trip distance in miles
    checks:
      - name: non_negative
  - name: total_fare
    type: double
    description: Total fare amount before tips and surcharges
    checks:
      - name: non_negative  
  - name: total_tips
    type: double
    description: Total tip amount collected
    checks:
      - name: non_negative
  - name: total_revenue
    type: double
    description: Total revenue including fare, tips, taxes, and surcharges
    checks:
      - name: non_negative   
  - name: avg_fare
    type: double
    description: Average fare amount per trip
  - name: avg_trip_distance
    type: double
    description: Average trip distance in miles per trip
  - name: avg_passengers
    type: double
    description: Average number of passengers per trip

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name,
    
    -- Count metrics
    COUNT(*) AS trip_count,
    SUM(COALESCE(passenger_count, 0)) AS total_passengers,
    
    -- Distance metrics
    SUM(COALESCE(trip_distance, 0)) AS total_distance,

    -- Revenue metrics
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    SUM(COALESCE(tip_amount, 0)) AS total_tips,
    SUM(COALESCE(total_amount, 0)) AS total_revenue,

    -- Average metrics
    AVG(COALESCE(fare_amount, 0)) AS avg_fare,
    AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
    AVG(COALESCE(passenger_count, 0)) AS avg_passengers

FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime <  '{{ end_datetime }}'

GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_name;