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

standardized as (
  select
    asin,
    reviewer_id,
    reviewer_name,
    overall,
    summary,
    review_text,
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(coalesce(review_text, ''), '<[^>]+>', ' '),
          '[[:cntrl:]]',
          ' '
        ),
        '\\s+',
        ' '
      )
    ) as review_text_clean,
    unix_review_time_str,
    review_time_str,
    verified_str,
    try_to_number(replace(vote, ',', '')) as vote_count,
    category,
    ingest_dt,
    source_file,
    load_ts
  from src
),

dated as (
  select
    *,
    coalesce(
      try_to_date(review_time_str),
      to_date(to_timestamp_ntz(try_to_number(unix_review_time_str)))
    ) as review_date,
    date_trunc('week', coalesce(
      try_to_date(review_time_str),
      to_date(to_timestamp_ntz(try_to_number(unix_review_time_str)))
    )) as week_bucket,
    date_trunc('month', coalesce(
      try_to_date(review_time_str),
      to_date(to_timestamp_ntz(try_to_number(unix_review_time_str)))
    )) as month_bucket,
    case
      when lower(verified_str) in ('true', 'y', 'yes') then 1
      when lower(verified_str) in ('false', 'n', 'no') then 0
      else null
    end as verified_flag
  from standardized
),

issue_flags as (
  select
    *,

    case
      when lower(review_text_clean) like '%broken%'
        or lower(review_text_clean) like '%damaged%'
        or lower(review_text_clean) like '%defective%'
        or lower(review_text_clean) like '%cracked%'
        or lower(review_text_clean) like '%shattered%'
        or lower(review_text_clean) like '%stopped working%'
        or lower(review_text_clean) like '%stop working%'
        or lower(review_text_clean) like '%did not work%'
        or lower(review_text_clean) like '%does not work%'
        or lower(review_text_clean) like '%not work%'
        or lower(review_text_clean) like '%would not work%'
        or lower(review_text_clean) like '%won''t work%'
        or lower(review_text_clean) like '%dead%'
        or lower(review_text_clean) like '%failed%'
      then 1 else 0
    end as has_damage_defect,

    case
      when lower(review_text_clean) like '%missing part%'
        or lower(review_text_clean) like '%missing parts%'
        or lower(review_text_clean) like '%missing piece%'
        or lower(review_text_clean) like '%missing pieces%'
        or lower(review_text_clean) like '%part missing%'
        or lower(review_text_clean) like '%parts missing%'
        or lower(review_text_clean) like '%incomplete%'
        or lower(review_text_clean) like '%not included%'
        or lower(review_text_clean) like '%did not include%'
        or lower(review_text_clean) like '%no charger%'
        or lower(review_text_clean) like '%no cable%'
        or lower(review_text_clean) like '%no remote%'
      then 1 else 0
    end as has_missing_parts,

    case
      when lower(review_text_clean) like '%never arrived%'
        or lower(review_text_clean) like '%not delivered%'
        or lower(review_text_clean) like '%delivered late%'
        or lower(review_text_clean) like '%arrived late%'
        or lower(review_text_clean) like '%late delivery%'
        or lower(review_text_clean) like '%late arrival%'
        or lower(review_text_clean) like '%delay in delivery%'
        or lower(review_text_clean) like '%package never arrived%'
        or lower(review_text_clean) like '%item never arrived%'
        or lower(review_text_clean) like '%did not receive%'
        or lower(review_text_clean) like '%not received%'
        or lower(review_text_clean) like '%received it late%'
        or lower(review_text_clean) like '%delivery delay%'
      then 1 else 0
    end as has_delivery_issue,

    case
      when lower(review_text_clean) like '%poor quality%'
        or lower(review_text_clean) like '%low quality%'
        or lower(review_text_clean) like '%bad quality%'
        or lower(review_text_clean) like '%cheaply made%'
        or lower(review_text_clean) like '%cheap%'
        or lower(review_text_clean) like '%flimsy%'
        or lower(review_text_clean) like '%not durable%'
        or lower(review_text_clean) like '%wear out%'
        or lower(review_text_clean) like '%wore out%'
      then 1 else 0
    end as has_quality_issue,

    case
      when lower(review_text_clean) like '%wrong item%'
        or lower(review_text_clean) like '%wrong product%'
        or lower(review_text_clean) like '%different item%'
        or lower(review_text_clean) like '%different product%'
        or lower(review_text_clean) like '%received the wrong%'
        or lower(review_text_clean) like '%sent the wrong%'
        or lower(review_text_clean) like '%not as described%'
        or lower(review_text_clean) like '%incorrect item%'
      then 1 else 0
    end as has_wrong_item,

    case
      when lower(review_text_clean) like '%shipping cost%'
        or lower(review_text_clean) like '%shipping costs%'
        or lower(review_text_clean) like '%charged for shipping%'
        or lower(review_text_clean) like '%charge for shipping%'
        or lower(review_text_clean) like '%seller%'
        or lower(review_text_clean) like '%customer service%'
        or lower(review_text_clean) like '%fake%'
        or lower(review_text_clean) like '%counterfeit%'
        or lower(review_text_clean) like '%bulk packaging%'
        or lower(review_text_clean) like '%scam%'
      then 1 else 0
    end as has_seller_fulfillment_noise

  from dated
),

derived as (
  select
    *,
    length(review_text_clean) as review_length,
    case when overall >= 4 then 1 else 0 end as positive_flag,
    case when overall = 3 then 1 else 0 end as neutral_flag,
    case when overall <= 2 then 1 else 0 end as negative_flag,

    array_to_string(
      array_construct_compact(
        iff(has_damage_defect = 1, 'damage_defect', null),
        iff(has_missing_parts = 1, 'missing_parts', null),
        iff(has_delivery_issue = 1, 'delivery_issue', null),
        iff(has_quality_issue = 1, 'quality_issue', null),
        iff(has_wrong_item = 1, 'wrong_item', null)
      ),
      ', '
    ) as issue_hint_keywords,

    case
      when has_damage_defect = 1 then 'damage_defect'
      when has_missing_parts = 1 then 'missing_parts'
      when has_delivery_issue = 1 then 'delivery_issue'
      when has_quality_issue = 1 then 'quality_issue'
      when has_wrong_item = 1 then 'wrong_item'
      else 'other'
    end as complaint_type_candidate,

    case when overall <= 3 then 1 else 0 end as is_low_rating_review
  from issue_flags
),

filtered as (
  select *
  from derived
  where asin is not null
    and category is not null
    and ingest_dt is not null
    and overall is not null
    and review_text_clean is not null
    and review_text_clean <> ''
    and review_length > 30
    and coalesce(has_seller_fulfillment_noise, 0) = 0
)

select
  asin,
  reviewer_id,
  reviewer_name,
  overall,
  summary,
  review_text,
  review_text_clean,
  unix_review_time_str,
  review_time_str,
  review_date,
  week_bucket,
  month_bucket,
  verified_str,
  verified_flag,
  vote_count,
  category,
  ingest_dt,
  review_length,
  positive_flag,
  neutral_flag,
  negative_flag,
  has_damage_defect,
  has_missing_parts,
  has_delivery_issue,
  has_quality_issue,
  has_wrong_item,
  has_seller_fulfillment_noise,
  issue_hint_keywords,
  complaint_type_candidate,
  is_low_rating_review,
  source_file,
  load_ts
from filtered