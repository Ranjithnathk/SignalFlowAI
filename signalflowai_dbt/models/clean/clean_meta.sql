with base as (
    select
        asin,
        nullif(trim(title), '') as title,
        nullif(trim(brand), '') as brand,
        try_to_double(regexp_replace(price, '[^0-9.]', '')) as price,
        nullif(trim(main_cat), '') as main_cat,
        nullif(trim(category_list), '') as category_list,
        nullif(trim(category), '') as category,
        ingest_dt,
        source_file,
        load_ts
    from {{ source('raw', 'meta_raw') }}
    where asin is not null
),

standardized as (
    select
        asin,
        title,
        initcap(brand) as brand,
        price,
        main_cat,
        category_list,
        category,

        try_parse_json(category_list) as category_array,

        try_parse_json(category_list)[0]::string as root_category,
        try_parse_json(category_list)[1]::string as sub_category,

        case
            when try_parse_json(category_list) is not null
                 and array_size(try_parse_json(category_list)) > 0
            then try_parse_json(category_list)[array_size(try_parse_json(category_list)) - 1]::string
            else null
        end as leaf_category,

        case
            when try_parse_json(category_list) is not null
            then array_size(try_parse_json(category_list))
            else null
        end as category_depth,

        case
            when try_parse_json(category_list) is not null
            then array_to_string(try_parse_json(category_list), ' > ')
            else null
        end as category_path,

        ingest_dt,
        source_file,
        load_ts
    from base
),

dedup as (
    select
        *,
        row_number() over (
            partition by asin
            order by ingest_dt desc, load_ts desc
        ) as rn
    from standardized
)

select
    asin,
    title,
    brand,
    price,
    main_cat,
    root_category,
    trim(sub_category) as sub_category,
    leaf_category,
    category_depth,
    category_path,
    category_list,
    category,
    ingest_dt,
    source_file,
    load_ts
from dedup
where rn = 1