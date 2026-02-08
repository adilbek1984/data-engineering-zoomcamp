--Creating an external table for all 6 files in 2024
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-adil-demo/yellow_tripdata_2024-*.parquet']
);


-- Creating a regular table based on an external table with unique_row_id
CREATE OR REPLACE TABLE `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024`
AS
SELECT
  MD5(CONCAT(
    COALESCE(CAST(VendorID AS STRING), ""),
    COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
    COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
    COALESCE(CAST(PULocationID AS STRING), ""),
    COALESCE(CAST(DOLocationID AS STRING), "")
  )) AS unique_row_id,
  "yellow_tripdata_2024" AS filename,
  *


--Question 1. What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(unique_row_id) FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024`

--Answer: 20,332,093

--Question 2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT PULocationID) FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024`;

SELECT COUNT(DISTINCT PULocationID) FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024_ext`;

--Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

--Question 3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
--Why are the estimated number of Bytes different?

SELECT PULocationID FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024`;

--Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query.
        --Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.