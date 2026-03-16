-- Part 4: Create BigQuery external tables
--
-- Create these tables in a dataset named `air_quality`.
-- Use wildcard URIs for the hourly data tables so a single table
-- spans all 31 days of files.
--
-- After creating the tables, verify they work by running:
--     SELECT count(*) FROM air_quality.<table_name>;


-- Hourly Observations — CSV
-- TODO: Create external table `hourly_observations_csv`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.csv

CREATE EXTERNAL TABLE air_quality.hourly_observations_csv (
  valid_date STRING,
  valid_time STRING,
  aqsid STRING,
  site_name STRING,
  gmt_offset STRING,
  parameter_name STRING,
  reporting_units STRING,
  value FLOAT64,
  data_source STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = ',',
  skip_leading_rows = 1,
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/*.csv']
);


-- Hourly Observations — JSON-L
-- TODO: Create external table `hourly_observations_jsonl`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.jsonl

CREATE EXTERNAL TABLE air_quality.hourly_observations_jsonl
OPTIONS (
  format = 'JSON',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/*.jsonl']
);


-- Hourly Observations — Parquet
-- TODO: Create external table `hourly_observations_parquet`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.parquet

CREATE EXTERNAL TABLE air_quality.hourly_observations_parquet
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/*.parquet']
);


-- Site Locations — CSV
-- TODO: Create external table `site_locations_csv`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.csv

CREATE EXTERNAL TABLE air_quality.site_locations_csv
OPTIONS (
  format = 'CSV',
  field_delimiter = ',',
  skip_leading_rows = 1,
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.csv']
);


-- Site Locations — JSON-L
-- TODO: Create external table `site_locations_jsonl`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.jsonl

CREATE EXTERNAL TABLE air_quality.site_locations_jsonl
OPTIONS (
  format = 'JSON',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.jsonl']
);


-- Site Locations — GeoParquet
-- TODO: Create external table `site_locations_geoparquet`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.geoparquet

CREATE EXTERNAL TABLE air_quality.site_locations_geoparquet
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.geoparquet']
);


-- Cross-table join query
-- Write a query that joins hourly observations with site locations
-- to get latitude/longitude for each observation. For example,
-- find the average PM2.5 value by state for a single day.

SELECT
  s.StateAbbreviation,
  AVG(h.value) AS avg_pm25
FROM air_quality.hourly_observations_csv h
JOIN air_quality.site_locations_csv s
ON h.aqsid = s.AQSID
WHERE h.parameter_name = 'PM2.5'
AND h.valid_date = '2024-07-01'
GROUP BY s.StateAbbreviation
ORDER BY avg_pm25 DESC;

