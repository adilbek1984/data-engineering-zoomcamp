/* @bruin
name: staging.test
type: bq.sql
connection: gcp-default

materialization:
  type: table
  strategy: create+replace
@bruin */

SELECT 1 as id