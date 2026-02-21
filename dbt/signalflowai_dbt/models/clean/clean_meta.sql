with src as (
  select
    nullif(trim(asin), '') as asin,
    nullif(trim(title), '') as title,
    nullif(trim(brand), '') as brand,
    price,
    nullif(trim(main_cat), '') as main_cat,
    nullif(trim(category_list), '') as category_list,
    nullif(trim(category), '') as category,
    ingest_dt,
    source_file,
    load_ts
  from {{ source('raw', 'meta_raw') }}
),

valid_asins as (
  select distinct asin
  from {{ ref('clean_reviews') }}
),

filtered as (
  select m.*
  from src m
  join valid_asins v
    on m.asin = v.asin
  where m.asin is not null
),

deduped as (
  select *
  from filtered
  qualify row_number() over (
    partition by asin
    order by ingest_dt desc, load_ts desc
  ) = 1
)

select
  asin,
  title,
  brand,
  price,
  main_cat,
  category_list,
  category,
  ingest_dt,
  source_file,
  load_ts
from deduped