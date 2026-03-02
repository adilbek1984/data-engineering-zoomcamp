--Question 1: What is the start date and end date of the dataset?
SELECT
  MIN(trip_dropoff_date_time),
  MAX(trip_dropoff_date_time)
FROM "trips";

--Answer: MIN(trip_dropoff_date_time) = 2009-06-01 11:48:00+00:00; MAX(trip_dropoff_date_time) = 2009-07-01 00:03:00+00:00



--Question 2: What proportion of trips are paid with credit card?

SELECT 
    COUNT(*) AS total_trips,
    SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) AS credit_card_trips,
    ROUND(
        100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) 
        / COUNT(*),
        2
    ) AS credit_card_percentage
FROM trips;

--Answer: total_trips = 10 000; credit_card_trips = 12 666; credit_card_percentage = 26,66%




--Question 3. What is the total amount of money generated in tips?


SELECT 
    ROUND(SUM(tip_amt), 2) AS total_tips
FROM trips

--Answer: total_tips = 6 063,41




