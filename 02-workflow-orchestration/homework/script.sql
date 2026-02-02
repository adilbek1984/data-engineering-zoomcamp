--Question #3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
SELECT COUNT(*) AS total_rows_2020
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020-%';


--Question #4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
SELECT COUNT(*) AS total_rows_2020
FROM public.green_tripdata
WHERE filename LIKE 'green_tripdata_2020-%';


--Question #5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
SELECT COUNT(*) AS total_rows_2020
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2021-03.csv';