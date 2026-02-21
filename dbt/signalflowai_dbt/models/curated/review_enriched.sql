select
  r.asin,
  r.category,
  r.ingest_dt,
  r.overall,
  r.review_length,
  r.positive_flag,
  r.neutral_flag,
  r.negative_flag,
  r.review_text,
  r.summary,
  m.title,
  m.brand,
  m.price,
  m.main_cat
from {{ ref('clean_reviews') }} r
left join {{ ref('clean_meta') }} m
  on r.asin = m.asin