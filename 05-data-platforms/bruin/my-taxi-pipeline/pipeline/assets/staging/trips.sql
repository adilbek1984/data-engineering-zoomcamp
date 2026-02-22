/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  # strategy: TODO
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  # incremental_key: TODO_SET_INCREMENTAL_KEY
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  # time_granularity: TODO_SET_GRANULARITY

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: pickup_datetime
    type: timestamp
    description: Timestamp when the meter was engaged at the start of the trip
    primary_key: true
  - name: dropoff_datetime
    type: timestamp
    description: Timestamp when the meter was disengaged at the end of the trip
    primary_key: true
  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone ID where the trip started
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone ID where the trip ended
    primary_key: true
  - name: fare_amount
    type: double
    description: The fare calculated based on time and distance before taxes and tips
    primary_key: true
    checks:
      - name: non_negative
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ['yellow', 'green']
  - name: payment_type_name
    type: varchar
    description: Human-readable name of the payment method from the lookup table
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: double
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total amount charged to the passenger including fare, taxes, tips, and surcharges
    checks:
      - name: non_negative

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

-- Staging query: clean, deduplicate, and enrich
WITH source_data AS (
    SELECT
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS pickup_datetime,
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
        pulocationid AS pickup_location_id,
        dolocationid AS dropoff_location_id,
        taxi_type,
        passenger_count,
        trip_distance,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        extracted_at
    FROM ingestion.trips
    WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0
),
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                fare_amount
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM source_data
)
SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.taxi_type,
    d.passenger_count,
    d.trip_distance,
    COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.improvement_surcharge,
    d.total_amount,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.row_num = 1;