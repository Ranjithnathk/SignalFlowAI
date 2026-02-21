with src as (
  select
    nullif(trim(asin), '') as asin,
    nullif(trim(reviewerid), '') as reviewer_id,
    nullif(trim(reviewername), '') as reviewer_name,
    try_to_double(overall) as overall,
    nullif(trim(summary), '') as summary,
    nullif(trim(reviewtext), '') as review_text,
    nullif(trim(unixreviewtime), '') as unix_review_time_str,
    nullif(trim(reviewtime), '') as review_time_str,
    nullif(trim(verified), '') as verified_str,
    nullif(trim(vote), '') as vote,
    nullif(trim(category), '') as category,
    ingest_dt,
    source_file,
    load_ts
  from {{ source('raw', 'reviews_raw') }}
),

derived as (
  select
    *,
    length(review_text) as review_length,
    case when overall >= 4 then 1 else 0 end as positive_flag,
    case when overall = 3 then 1 else 0 end as neutral_flag,
    case when overall <= 2 then 1 else 0 end as negative_flag
  from src
),

filtered as (
  select *
  from derived
  where asin is not null
    and category is not null
    and ingest_dt is not null
    and overall is not null
    and review_text is not null
    and review_length > 30
)

select
  asin,
  reviewer_id,
  reviewer_name,
  overall,
  summary,
  review_text,
  unix_review_time_str,
  review_time_str,
  verified_str,
  vote,
  category,
  ingest_dt,
  review_length,
  positive_flag,
  neutral_flag,
  negative_flag,
  source_file,
  load_ts
from filtered