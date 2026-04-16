USE ROLE TRAINING_ROLE;
USE WAREHOUSE SIGNALFLOWAI_ETL_WH;
USE DATABASE SIGNALFLOWAI_PROD_DB;
USE SCHEMA RAW;

-- Create tables once (no wipe)
CREATE TABLE IF NOT EXISTS RAW.REVIEWS_RAW (
  asin             STRING,
  reviewerID        STRING,
  reviewerName      STRING,
  overall           FLOAT,
  summary           STRING,
  reviewText        STRING,
  unixReviewTime    STRING,
  reviewTime        STRING,
  verified          STRING,
  vote              STRING,
  category          STRING,
  ingest_dt         DATE,
  source_file       STRING,
  load_ts           TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS RAW.META_RAW (
  asin            STRING,
  title           STRING,
  brand           STRING,
  price           FLOAT,
  main_cat        STRING,
  category_list   STRING,
  category        STRING,
  ingest_dt       DATE,
  source_file     STRING,
  load_ts         TIMESTAMP_NTZ
);

-- load audit
CREATE TABLE IF NOT EXISTS RAW.LOAD_AUDIT (
  run_ts           TIMESTAMP_NTZ,
  target_table     STRING,
  files_loaded     NUMBER,
  rows_loaded      NUMBER,
  first_error      STRING
);

SET run_ts = CURRENT_TIMESTAMP();

-- Load REVIEWS 
COPY INTO RAW.REVIEWS_RAW
FROM (
  SELECT
    $1:asin::STRING,
    $1:reviewerID::STRING,
    $1:reviewerName::STRING,
    $1:overall::FLOAT,
    $1:summary::STRING,
    $1:reviewText::STRING,
    $1:unixReviewTime::STRING,
    $1:reviewTime::STRING,
    $1:verified::STRING,
    $1:vote::STRING,
    $1:category::STRING,
    TO_DATE($1:ingest_dt::STRING) AS ingest_dt,
    METADATA$FILENAME AS source_file,
    CURRENT_TIMESTAMP() AS load_ts
  FROM @SIGNALFLOWAI_PROD_DB.RAW.S3_LANDING_STAGE/reviews/
)
FILE_FORMAT = (FORMAT_NAME = SIGNALFLOWAI_PROD_DB.RAW.PARQUET_FF);

-- Audit REVIEWS load using COPY_HISTORY
INSERT INTO RAW.LOAD_AUDIT
SELECT
  $run_ts,
  'RAW.REVIEWS_RAW' AS target_table,
  COUNT(DISTINCT file_name) AS files_loaded,
  COALESCE(SUM(row_count), 0) AS rows_loaded,
  MIN(first_error_message) AS first_error
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'REVIEWS_RAW',
    START_TIME => DATEADD('minute', -240, $run_ts),
    END_TIME   => DATEADD('minute',  240, $run_ts)
  )
);

-- Load META 
COPY INTO RAW.META_RAW
FROM (
  SELECT
    $1:asin::STRING,
    $1:title::STRING,
    $1:brand::STRING,
    $1:price::FLOAT,
    $1:main_cat::STRING,
    $1:category_list::STRING,
    $1:category::STRING,
    TO_DATE($1:ingest_dt::STRING) AS ingest_dt,
    METADATA$FILENAME AS source_file,
    CURRENT_TIMESTAMP() AS load_ts
  FROM @SIGNALFLOWAI_PROD_DB.RAW.S3_LANDING_STAGE/meta/
)
FILE_FORMAT = (FORMAT_NAME = SIGNALFLOWAI_PROD_DB.RAW.PARQUET_FF);

-- Audit META load using COPY_HISTORY
INSERT INTO RAW.LOAD_AUDIT
SELECT
  $run_ts,
  'RAW.META_RAW' AS target_table,
  COUNT(DISTINCT file_name) AS files_loaded,
  COALESCE(SUM(row_count), 0) AS rows_loaded,
  MIN(first_error_message) AS first_error
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'META_RAW',
    START_TIME => DATEADD('minute', -240, $run_ts),
    END_TIME   => DATEADD('minute',  240, $run_ts)
  )
);

-- Quick operational checks
SELECT COUNT(*) AS reviews_cnt FROM RAW.REVIEWS_RAW;
SELECT COUNT(*) AS meta_cnt FROM RAW.META_RAW;

SELECT category, ingest_dt, COUNT(*) AS cnt
FROM SIGNALFLOWAI_PROD_DB.RAW.REVIEWS_RAW
GROUP BY 1,2
ORDER BY ingest_dt DESC, category;

SELECT * FROM RAW.LOAD_AUDIT ORDER BY run_ts DESC;

SELECT source_file, COUNT(*) AS rows_in_table
FROM SIGNALFLOWAI_PROD_DB.RAW.REVIEWS_RAW
GROUP BY 1
HAVING COUNT(*) > 200000  -- part files should be ~200k, last part less
ORDER BY rows_in_table DESC
LIMIT 20;

DELETE FROM SIGNALFLOWAI_PROD_DB.RAW.REVIEWS_RAW WHERE category = 'dev';
DELETE FROM SIGNALFLOWAI_PROD_DB.RAW.META_RAW WHERE category = 'dev';