USE ROLE TRAINING_ROLE;
USE WAREHOUSE SIGNALFLOWAI_ETL_WH;
USE DATABASE SIGNALFLOWAI_DB;
USE SCHEMA RAW;

-- 1) Create RAW tables
-- Reviews RAW (from Parquet landing)
CREATE OR REPLACE TABLE RAW.REVIEWS_RAW (
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
  review_length     NUMBER,

  positive_flag     NUMBER,
  neutral_flag      NUMBER,
  negative_flag     NUMBER,

  source_file       STRING,
  load_ts           TIMESTAMP_NTZ
);

-- Meta RAW (from Parquet landing)
CREATE OR REPLACE TABLE RAW.META_RAW (
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

-- 2) COPY INTO REVIEWS
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
    $1:review_length::NUMBER,

    $1:positive_flag::NUMBER,
    $1:neutral_flag::NUMBER,
    $1:negative_flag::NUMBER,

    METADATA$FILENAME AS source_file,
    CURRENT_TIMESTAMP() AS load_ts
  FROM @SIGNALFLOWAI_DB.RAW.S3_LANDING_STAGE/reviews/
)
FILE_FORMAT = (FORMAT_NAME = SIGNALFLOWAI_DB.RAW.PARQUET_FF);


-- 3) COPY INTO META
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
  FROM @SIGNALFLOWAI_DB.RAW.S3_LANDING_STAGE/meta/
)
FILE_FORMAT = (FORMAT_NAME = SIGNALFLOWAI_DB.RAW.PARQUET_FF);

-- 4) Validation Queries
SELECT COUNT(*) FROM RAW.REVIEWS_RAW;
SELECT COUNT(*) FROM RAW.META_RAW;

SELECT category, ingest_dt, COUNT(*) 
FROM RAW.REVIEWS_RAW 
GROUP BY 1,2
ORDER BY ingest_dt DESC;

SELECT category, ingest_dt, COUNT(*) 
FROM RAW.META_RAW
GROUP BY 1,2
ORDER BY ingest_dt DESC;





-- verification checks
-- 1. Partition sanity (category + ingest_dt)
SELECT category, ingest_dt, COUNT(*) AS cnt
FROM RAW.REVIEWS_RAW
GROUP BY 1,2
ORDER BY ingest_dt DESC, category;

SELECT category, ingest_dt, COUNT(*) AS cnt
FROM RAW.META_RAW
GROUP BY 1,2
ORDER BY ingest_dt DESC, category;

-- 2. Null checks on keys (critical)
SELECT
  SUM(CASE WHEN asin IS NULL THEN 1 ELSE 0 END) AS null_asin,
  COUNT(*) AS total
FROM RAW.REVIEWS_RAW;

SELECT
  SUM(CASE WHEN asin IS NULL THEN 1 ELSE 0 END) AS null_asin,
  COUNT(*) AS total
FROM RAW.META_RAW;

-- 3) Rating distribution (be 1–5)
SELECT overall, COUNT(*) AS cnt
FROM RAW.REVIEWS_RAW
GROUP BY 1
ORDER BY 1;

-- 4) Flag integrity check 
SELECT
  SUM(CASE WHEN overall IS NOT NULL AND (positive_flag + neutral_flag + negative_flag) != 1 THEN 1 ELSE 0 END) AS bad_rows,
  SUM(CASE WHEN overall IS NULL AND (positive_flag + neutral_flag + negative_flag) != 0 THEN 1 ELSE 0 END) AS bad_null_rows
FROM RAW.REVIEWS_RAW;

-- 5) Join coverage (reviews ASIN exists in meta?)
SELECT
  COUNT(*) AS total_reviews,
  COUNT(DISTINCT r.asin) AS distinct_review_asins,
  COUNT(DISTINCT m.asin) AS distinct_meta_asins,
  COUNT(DISTINCT CASE WHEN m.asin IS NOT NULL THEN r.asin END) AS matched_review_asins
FROM RAW.REVIEWS_RAW r
LEFT JOIN RAW.META_RAW m
  ON r.asin = m.asin;