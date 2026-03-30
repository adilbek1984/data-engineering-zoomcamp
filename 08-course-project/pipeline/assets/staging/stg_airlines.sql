/* @bruin
name: staging.stg_airlines
type: bq.sql
connection: gcp-default

materialization:
  type: table
  strategy: create+replace
@bruin */

-- Basic cleaning: ensuring no empty rows and standardizing names
SELECT 
    IATA_CODE as airline_id,
    AIRLINE as airline_name
FROM `kestra-sandbox-486404.staging.ext_airlines`
WHERE IATA_CODE IS NOT NULL