/* @bruin
name: staging.stg_airports
type: bq.sql
connection: gcp-default
materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 
    IATA_CODE as airport_id,
    AIRPORT as airport_name,
    CITY as city,
    STATE as state,
    LATITUDE as latitude,
    LONGITUDE as longitude
FROM `kestra-sandbox-486404.staging.ext_airports`
WHERE IATA_CODE IS NOT NULL