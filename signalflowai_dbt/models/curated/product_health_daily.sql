select
  review_date,
  week_bucket,
  month_bucket,
  category,
  brand,
  asin,
  complaint_type_candidate,
  count(*) as review_count,
  sum(negative_flag) as negative_reviews,
  sum(neutral_flag) as neutral_reviews,
  sum(positive_flag) as positive_reviews,
  round((sum(negative_flag) / nullif(count(*), 0))::float, 6) as negative_rate,
  round((sum(positive_flag) / nullif(count(*), 0))::float, 6) as positive_rate,
  avg(overall) as avg_rating
from {{ ref('review_enriched') }}
group by 1,2,3,4,5,6,7