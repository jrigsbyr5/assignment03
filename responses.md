# Assignment 03 Responses

## Part 4: BigQuery External Tables

### Hourly Observations — CSV External Table SQL

```sql
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
```

### Hourly Observations — JSON-L External Table SQL

```sql
CREATE EXTERNAL TABLE air_quality.hourly_observations_jsonl
OPTIONS (
  format = 'JSON',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/*.jsonl']
);
```

### Hourly Observations — Parquet External Table SQL

```sql
CREATE EXTERNAL TABLE air_quality.hourly_observations_parquet
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/*.parquet']
);
```

### Site Locations — CSV External Table SQL

```sql
CREATE EXTERNAL TABLE air_quality.site_locations_csv
OPTIONS (
  format = 'CSV',
  field_delimiter = ',',
  skip_leading_rows = 1,
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.csv']
);
```

### Site Locations — JSON-L External Table SQL

```sql
CREATE EXTERNAL TABLE air_quality.site_locations_jsonl
OPTIONS (
  format = 'JSON',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.jsonl']
);
```

### Site Locations — GeoParquet External Table SQL

```sql
CREATE EXTERNAL TABLE air_quality.site_locations_geoparquet
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/sites/site_locations.geoparquet']
);
```

### Cross-Table Join Query

```sql
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
```

---

## Part 5: Hive-Partitioned External Tables

### Hourly Observations — CSV (hive-partitioned)

```sql
CREATE EXTERNAL TABLE air_quality.hourly_observations_csv_hive (
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
WITH PARTITION COLUMNS (
  airnow_date STRING
)
OPTIONS (
  format = 'CSV',
  field_delimiter = ',',
  skip_leading_rows = 1,
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/csv/*'],
  hive_partition_uri_prefix = 'gs://joshr-musa-5090-assignment3/air_quality/hourly/csv/'
);
```

### Hourly Observations — JSON-L (hive-partitioned)

```sql
CREATE EXTERNAL TABLE air_quality.hourly_observations_jsonl_hive
WITH PARTITION COLUMNS (
  airnow_date STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/jsonl/*'],
  hive_partition_uri_prefix = 'gs://joshr-musa-5090-assignment3/air_quality/hourly/jsonl/'
);
```

### Hourly Observations — Parquet (hive-partitioned)

```sql
CREATE EXTERNAL TABLE air_quality.hourly_observations_parquet_hive
WITH PARTITION COLUMNS (
  airnow_date STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://joshr-musa-5090-assignment3/air_quality/hourly/parquet/*'],
  hive_partition_uri_prefix = 'gs://joshr-musa-5090-assignment3/air_quality/hourly/parquet/'
);
```

---

## Part 6: Analysis & Reflection

### 1. File Sizes

**Hourly data (single day):**

| Format  | File Size |
|---------|-----------|
| CSV     | ~2.5 MB |
| JSON-L  | ~3.9 MB |
| Parquet | ~1.1 MB |

**Site locations:**

| Format     | File Size |
|------------|-----------|
| CSV        | ~450 KB |
| JSON-L     | ~650 KB |
| GeoParquet | ~230 KB |

**Analysis:**
> The Parquet files were the smallest format, and the JSON-L files were the largest. I believe the JSON-L files are larger because each record contains repeated field names and additional formatting. Parquet is the smallest because it's compressed, which allows repeated values in columns to be stored efficiently.

### 2. Format Anatomy

> A CSV is a plain-text tabular format where each row represents a record and columns are separated by commas. It is simple and does not contain metadata about column types or schema. Because it is row-based, query engines must scan the entire dataset even if only a few columns are needed.

> A Parquet is a binary columnar storage format designed for analytics systems. Instead of storing rows sequentially, it stores each column separately along with metadata describing the schema and compression. This allows systems like BigQuery to read only the columns needed for a query, significantly improving performance and reducing the amount of data scanned.

### 3. Choosing Formats for BigQuery

> Parquet is preferred for BigQuery external tables because it is optimized for analytical workloads. Its columnar storage allows BigQuery to scan only the columns referenced in a query rather than the entire dataset, reducing query execution time and number of bytes processed, lowering cost. 

### 4. Pipeline vs. Warehouse Joins

> This approach keeps the pipeline simpler and avoids duplicating the same geographic metadata across millions of hourly observations. It also allows updates to site metadata without requiring regeneration of the entire dataset.

> The alternative approach simplifies queries because no join is required during analysis. However, it increases storage usage and duplicates the site information across every observation record. Usually, normalized storage with warehouse joins is preferred when the metadata table is relatively small and stable.

#### Stretch Challenge (optional)

If you implemented the stretch challenge (scripts `06_prepare`, `06_upload_to_gcs`, `06_create_tables.sql`), paste your SQL statements here:

```sql
-- Merged Hourly + Sites — CSV (hive-partitioned)
```

```sql
-- Merged Hourly + Sites — JSON-L (hive-partitioned)
```

```sql
-- Merged Hourly + Sites — GeoParquet (hive-partitioned)
```

### 5. Choosing a Data Source

For each person below, which air quality data source (AirNow hourly files, AirNow API, AQS bulk downloads, or AQS API) would you recommend, and why?

**a) A parent who wants a dashboard showing current air quality near their child's school:**
> I would recommend the AirNow API because it provides real-time air quality measurements. A dashboard for a parent needs current conditions rather than historical datasets, so an API that returns the most recent monitoring data is the most appropriate solution.

**b) An environmental justice advocate identifying neighborhoods with chronically poor air quality over the past decade:**
> I would recommend the AQS bulk downloads because they contain large historical datasets spanning many years. This allows analysis of long-term air quality patterns and trends across different locations, which is necessary for identifying persistent environmental health problems.

**c) A school administrator who needs automated morning alerts when AQI exceeds a threshold:**
> I would recommend the AirNow API because it provides up-to-date measurements that can be queried automatically each morning. A script could check the API daily and trigger alerts if the AQI exceeds a predefined threshold.