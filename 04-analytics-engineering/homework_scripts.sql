-- Question 3. Counting Records in fct_monthly_zone_revenue

SELECT
  COUNT(*) AS num_of_records,
FROM `kestra-sandbox-486404.zoomcamp.fct_monthly_zone_revenue`;

--Answer: 12,184


-- Question 4. Best Performing Zone for Green Taxis (2020)

SELECT
  MAX(revenue_monthly_total_amount) AS highest_total_revenue,
  pickup_zone
FROM `kestra-sandbox-486404.zoomcamp.fct_monthly_zone_revenue`
WHERE
  service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY highest_total_revenue DESC
LIMIT 1;

--Answer: East Harlem North (434555.26)

-- Question 5. Green Taxi Trip Counts (October 2019)

SELECT
  SUM(total_monthly_trips) AS total_monthly_trips,
FROM `kestra-sandbox-486404.zoomcamp.fct_monthly_zone_revenue`
WHERE
  service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10;

--Answer: Total monthly trips 384,624


--Question 6. Build a Staging Model for FHV Data. What is the count of records in stg_fhv_tripdata?

SELECT
  COUNT(*) AS num_of_records
FROM `kestra-sandbox-486404.zoomcamp.stg_fhv_tripdata`;

--Answer: 43,244,693