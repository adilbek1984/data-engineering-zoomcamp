--YELLOW TRIP DATASET

--Creating an external table for all 12 files in 2020
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2020_ext`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-adil-demo/yellow_tripdata_2020-*.csv'],
  skip_leading_rows = 1
);

-- Creating a regular table based on an external table with unique_row_id
CREATE OR REPLACE TABLE `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2020`
AS
SELECT
  MD5(CONCAT(
    COALESCE(CAST(VendorID AS STRING), ""),
    COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
    COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
    COALESCE(CAST(PULocationID AS STRING), ""),
    COALESCE(CAST(DOLocationID AS STRING), "")
  )) AS unique_row_id,

  "yellow_tripdata_2020" AS filename,

  CAST(VendorID AS STRING) AS VendorID,
  CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
  CAST(passenger_count AS INT64) AS passenger_count,
  CAST(trip_distance AS NUMERIC) AS trip_distance,
  CAST(RatecodeID AS STRING) AS RatecodeID,
  CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
  CAST(PULocationID AS STRING) AS PULocationID,
  CAST(DOLocationID AS STRING) AS DOLocationID,
  CAST(payment_type AS INT64) AS payment_type,
  CAST(fare_amount AS NUMERIC) AS fare_amount,
  CAST(extra AS NUMERIC) AS extra,
  CAST(mta_tax AS NUMERIC) AS mta_tax,
  CAST(tip_amount AS NUMERIC) AS tip_amount,
  CAST(tolls_amount AS NUMERIC) AS tolls_amount,
  CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
  CAST(total_amount AS NUMERIC) AS total_amount,
  CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge

FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2020_ext`;


--Merging tables to table yellow_tripdata
MERGE INTO `kestra-sandbox-486404.zoomcamp.yellow_tripdata` T
USING (
    SELECT * FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2019`
    UNION ALL
    SELECT * FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2020`
) S
ON T.unique_row_id = S.unique_row_id

WHEN NOT MATCHED THEN
INSERT (
    unique_row_id,
    filename,
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge
)
VALUES (
    S.unique_row_id,
    S.filename,
    S.VendorID,
    S.tpep_pickup_datetime,
    S.tpep_dropoff_datetime,
    S.passenger_count,
    S.trip_distance,
    S.RatecodeID,
    S.store_and_fwd_flag,
    S.PULocationID,
    S.DOLocationID,
    S.payment_type,
    S.fare_amount,
    S.extra,
    S.mta_tax,
    S.tip_amount,
    S.tolls_amount,
    S.improvement_surcharge,
    S.total_amount,
    S.congestion_surcharge
);



--GREEN TRIP DATASET

--Creating an external table for all 12 files in 2020
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486404.zoomcamp.green_tripdata_2020_ext`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-adil-demo/green_tripdata_2020-*.csv'],
  skip_leading_rows = 1
);

-- Creating a regular table based on an external table with unique_row_id
CREATE OR REPLACE TABLE `kestra-sandbox-486404.zoomcamp.green_tripdata_2020`
AS
SELECT
  MD5(CONCAT(
    COALESCE(CAST(VendorID AS STRING), ""),
    COALESCE(CAST(lpep_pickup_datetime AS STRING), ""),
    COALESCE(CAST(lpep_dropoff_datetime AS STRING), ""),
    COALESCE(CAST(PULocationID AS STRING), ""),
    COALESCE(CAST(DOLocationID AS STRING), "")
  )) AS unique_row_id,

  "green_tripdata_2020" AS filename,

  CAST(VendorID AS STRING) AS VendorID,
  CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime,
  CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime,
  CAST(passenger_count AS INT64) AS passenger_count,
  CAST(trip_distance AS NUMERIC) AS trip_distance,
  CAST(RatecodeID AS STRING) AS RatecodeID,
  CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
  CAST(PULocationID AS STRING) AS PULocationID,
  CAST(DOLocationID AS STRING) AS DOLocationID,
  CAST(payment_type AS INT64) AS payment_type,
  CAST(fare_amount AS NUMERIC) AS fare_amount,
  CAST(extra AS NUMERIC) AS extra,
  CAST(mta_tax AS NUMERIC) AS mta_tax,
  CAST(tip_amount AS NUMERIC) AS tip_amount,
  CAST(tolls_amount AS NUMERIC) AS tolls_amount,
  CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
  CAST(total_amount AS NUMERIC) AS total_amount,
  CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge

FROM `kestra-sandbox-486404.zoomcamp.green_tripdata_2020_ext`;

--Merging tables to table green_tripdata
MERGE INTO `kestra-sandbox-486404.zoomcamp.green_tripdata` T
USING (
    SELECT * FROM `kestra-sandbox-486404.zoomcamp.green_tripdata_2019`
    UNION ALL
    SELECT * FROM `kestra-sandbox-486404.zoomcamp.green_tripdata_2020`
) S
ON T.unique_row_id = S.unique_row_id

WHEN NOT MATCHED THEN
INSERT (
    unique_row_id,
    filename,
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge
)
VALUES (
    S.unique_row_id,
    S.filename,
    S.VendorID,
    S.lpep_pickup_datetime,
    S.lpep_dropoff_datetime,
    S.passenger_count,
    S.trip_distance,
    S.RatecodeID,
    S.store_and_fwd_flag,
    S.PULocationID,
    S.DOLocationID,
    S.payment_type,
    S.fare_amount,
    S.extra,
    S.mta_tax,
    S.tip_amount,
    S.tolls_amount,
    S.improvement_surcharge,
    S.total_amount,
    S.congestion_surcharge
);


--FHV TRIP DATASET

--Creating an external table for all 12 files in 2019
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486404.zoomcamp.fhv_tripdata_2019_ext`
OPTIONS (
  format = 'CSV',
  uris = ['gs://kestra-zoomcamp-adil-demo/fhv_tripdata_2019-*.csv'],
  skip_leading_rows = 1
);

-- Creating a regular table based on an external table with unique_row_id
CREATE OR REPLACE TABLE `kestra-sandbox-486404.zoomcamp.fhv_tripdata_2019`
AS
SELECT
  MD5(CONCAT(
    COALESCE(CAST(dispatching_base_num AS STRING), ""),
    COALESCE(CAST(pickup_datetime AS STRING), ""),
    COALESCE(CAST(dropOff_datetime AS STRING), ""),
    COALESCE(CAST(PUlocationID AS STRING), ""),
    COALESCE(CAST(DOlocationID AS STRING), "")
  )) AS unique_row_id,

  "fhv_tripdata_2019" AS filename,

  CAST(dispatching_base_num AS STRING) AS dispatching_base_num,
  CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
  CAST(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime,
  CAST(PUlocationID AS INT64) AS PUlocationID,
  CAST(DOlocationID AS INT64) AS DOlocationID,
  CAST(SR_Flag AS STRING) AS SR_Flag,
  CAST(Affiliated_base_number AS STRING) AS Affiliated_base_number

FROM `kestra-sandbox-486404.zoomcamp.fhv_tripdata_2019_ext`;


-- Creating fhv_tripdata table
CREATE TABLE IF NOT EXISTS `kestra-sandbox-486404.zoomcamp.fhv_tripdata`
(
    unique_row_id BYTES OPTIONS (description = 'A unique identifier for the trip, generated by hashing key trip attributes.'),
    filename STRING OPTIONS (description = 'The source filename from which the trip data was loaded.'),

    dispatching_base_num STRING OPTIONS (description = 'The TLC base license number of the dispatching base.'),
    pickup_datetime TIMESTAMP OPTIONS (description = 'The date and time when the trip started.'),
    dropOff_datetime TIMESTAMP OPTIONS (description = 'The date and time when the trip ended.'),

    PUlocationID INT64 OPTIONS (description = 'TLC Taxi Zone where the trip started.'),
    DOlocationID INT64 OPTIONS (description = 'TLC Taxi Zone where the trip ended.'),

    SR_Flag STRING OPTIONS (description = 'Indicates if the trip was a shared ride.'),
    Affiliated_base_number STRING OPTIONS (description = 'The affiliated base license number.')
)
PARTITION BY DATE(pickup_datetime);

-- Merging table fhv_tripdata_2019 into fhv_tripdata
MERGE INTO `kestra-sandbox-486404.zoomcamp.fhv_tripdata` T
USING (
    SELECT *
    FROM `kestra-sandbox-486404.zoomcamp.fhv_tripdata_2019`
) S
ON T.unique_row_id = S.unique_row_id

WHEN NOT MATCHED THEN
INSERT (
    unique_row_id,
    filename,
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
)
VALUES (
    S.unique_row_id,
    S.filename,
    S.dispatching_base_num,
    S.pickup_datetime,
    S.dropOff_datetime,
    S.PUlocationID,
    S.DOlocationID,
    S.SR_Flag,
    S.Affiliated_base_number
);