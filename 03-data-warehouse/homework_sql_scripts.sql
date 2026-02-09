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

SELECT PULocationID, DOLocationID FROM `kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024`;

--Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query.
        --Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


--Question 4. How many records have a fare_amount of 0?

SELECT COUNT(*) FROM kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024 WHERE fare_amount = 0.0;

--Answer: 8,333


--Question 5. Partitioning and clustering. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024_partitioned_clustered
PARTITION BY
DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024;

--Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID


--Question 6. Partition benefits. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

-- Query scans 310.24 MB
SELECT COUNT(DISTINCT VendorID)
FROM kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query scans 26.84 MB
SELECT COUNT(DISTINCT VendorID)
FROM kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

--Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


--Question 7. External table storage. Where is the data stored in the External Table you created?

--Answer: GCP Bucket


--Question 9. Understanding table scans. Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT COUNT(*) FROM kestra-sandbox-486404.zoomcamp.yellow_tripdata_2024;

--Answer: This query will process 0 B when run.

--Why: The query estimates 0 B processed because BigQuery retrieves the total row count from table metadata instead of scanning the underlying data. Since no columns or filters are applied, the query can be answered using internal statistics without reading any data blocks.