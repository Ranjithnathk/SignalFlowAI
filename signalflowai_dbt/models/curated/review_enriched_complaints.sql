with base as (
    select *
    from {{ ref('review_enriched') }}
    where category in ('electronics', 'home_kitchen')
      and overall <= 3
      and complaint_type_candidate <> 'other'
),

prepared as (
    select
        *,
        lower(coalesce(review_text_clean, '')) as review_text_lc,
        lower(coalesce(summary, '')) as summary_lc,
        lower(coalesce(title, '')) as title_lc,
        lower(coalesce(main_cat, '')) as main_cat_lc,
        lower(coalesce(sub_category, '')) as sub_category_lc,
        lower(coalesce(root_category, '')) as root_category_lc,
        lower(coalesce(leaf_category, '')) as leaf_category_lc,
        lower(coalesce(category_path, '')) as category_path_lc
    from base
),

gated as (
    select
        *,

        case
            when complaint_type_candidate = 'damage_defect' and has_damage_defect = 1 then 1
            when complaint_type_candidate = 'missing_parts' and has_missing_parts = 1 then 1
            when complaint_type_candidate = 'delivery_issue' and has_delivery_issue = 1 then 1
            when complaint_type_candidate = 'quality_issue' and has_quality_issue = 1 then 1
            when complaint_type_candidate = 'wrong_item' and has_wrong_item = 1 then 1
            else 0
        end as operational_complaint_flag,

        /* Light metadata sanity check: allow proper domain matches, but don't wipe out rows if metadata is sparse */
        case
            when category = 'electronics'
                 and (
                     root_category_lc = 'electronics'
                     or category_path_lc like 'electronics%'
                     or main_cat_lc like '%electronics%'
                     or sub_category_lc like '%electronics%'
                     or main_cat is null
                 )
            then 1

            when category = 'home_kitchen'
                 and (
                     root_category_lc in ('home & kitchen', 'home and kitchen')
                     or category_path_lc like 'home & kitchen%'
                     or category_path_lc like 'home and kitchen%'
                     or main_cat_lc like '%kitchen%'
                     or main_cat_lc like '%home%'
                     or sub_category_lc like '%kitchen%'
                     or sub_category_lc like '%home%'
                     or main_cat is null
                 )
            then 1

            else 0
        end as metadata_domain_match_flag,

        /* Exclude obvious narrative/content-review noise */
        case
            when regexp_like(
                review_text_lc,
                '(boring novel|boring book|poor writing|bad writing|story line|storyline|plot|characters|author|chapter|hardcover|paperback|read this book|novel i''ve read)'
            )
            then 1
            else 0
        end as narrative_content_noise_flag,

        /* Add second-level subtype for precision without changing top-level complaint taxonomy */
        case
            when complaint_type_candidate = 'damage_defect'
                 and (
                     review_text_lc like '%not compatible%'
                     or review_text_lc like '%incompatible%'
                     or review_text_lc like '%does not fit%'
                     or review_text_lc like '%did not fit%'
                     or review_text_lc like '%doesn''t fit%'
                     or review_text_lc like '%not recognized%'
                     or review_text_lc like '%not supported%'
                     or review_text_lc like '%won''t fit%'
                     or review_text_lc like '%would not fit%'
                     or review_text_lc like '%interferes with%'
                     or review_text_lc like '%fits but%'
                     or review_text_lc like '%not work with%'
                     or review_text_lc like '%did not work with%'
                     or review_text_lc like '%work with my%'
                     or review_text_lc like '%works with my%'
                     or review_text_lc like '%not compatible with%'
                     or review_text_lc like '%does not work with%'
                 )
            then 'compatibility_fit_issue'

            when complaint_type_candidate = 'damage_defect'
                 and (
                     review_text_lc like '%fake%'
                     or review_text_lc like '%counterfeit%'
                     or review_text_lc like '%phony%'
                     or review_text_lc like '%scam%'
                     or review_text_lc like '%not genuine%'
                 )
            then 'counterfeit_or_fake'
            
            when complaint_type_candidate = 'damage_defect'
                 and (
                     review_text_lc like '%broken%'
                     or review_text_lc like '%cracked%'
                     or review_text_lc like '%shattered%'
                     or review_text_lc like '%burnt%'
                     or review_text_lc like '%burned%'
                     or review_text_lc like '%dead%'
                     or review_text_lc like '%defective%'
                     or review_text_lc like '%failed%'
                     or review_text_lc like '%stopped working%'
                     or review_text_lc like '%stop working%'
                     or review_text_lc like '%would not work%'
                     or review_text_lc like '%won''t work%'
                     or review_text_lc like '%does not work%'
                     or review_text_lc like '%did not work%'
                     or review_text_lc like '%malfunction%'
                     or review_text_lc like '%freezing up%'
                     or review_text_lc like '%black screen%'
                 )
            then 'hard_defect'

            when complaint_type_candidate = 'delivery_issue'
                 and (
                     review_text_lc like '%never arrived%'
                     or review_text_lc like '%not received%'
                     or review_text_lc like '%did not receive%'
                     or review_text_lc like '%package never arrived%'
                     or review_text_lc like '%item never arrived%'
                 )
            then 'not_received'

            when complaint_type_candidate = 'delivery_issue'
                 and (
                     review_text_lc like '%arrived late%'
                     or review_text_lc like '%delivered late%'
                     or review_text_lc like '%late delivery%'
                     or review_text_lc like '%late arrival%'
                     or review_text_lc like '%delivery delay%'
                 )
            then 'late_delivery'

            when complaint_type_candidate = 'delivery_issue'
                 and (
                     review_text_lc like '%shipping cost%'
                     or review_text_lc like '%shipping costs%'
                     or review_text_lc like '%charged for shipping%'
                     or review_text_lc like '%charge for shipping%'
                 )
            then 'shipping_cost_issue'

            when complaint_type_candidate = 'missing_parts'
                 and (
                     review_text_lc like '%no charger%'
                     or review_text_lc like '%no cable%'
                     or review_text_lc like '%no remote%'
                     or review_text_lc like '%missing remote%'
                     or review_text_lc like '%missing screw%'
                     or review_text_lc like '%missing screws%'
                     or review_text_lc like '%missing adapter%'
                     or review_text_lc like '%adapter not included%'
                     or review_text_lc like '%not included%'
                 )
            then 'accessory_missing'

            when complaint_type_candidate = 'missing_parts'
                 and (
                     review_text_lc like '%missing part%'
                     or review_text_lc like '%missing parts%'
                     or review_text_lc like '%missing piece%'
                     or review_text_lc like '%missing pieces%'
                     or review_text_lc like '%incomplete%'
                 )
            then 'core_parts_missing'

            when complaint_type_candidate = 'quality_issue'
                 and (
                     review_text_lc like '%poor quality%'
                     or review_text_lc like '%low quality%'
                     or review_text_lc like '%bad quality%'
                     or review_text_lc like '%cheaply made%'
                     or review_text_lc like '%cheap%'
                     or review_text_lc like '%flimsy%'
                 )
            then 'poor_build_quality'

            when complaint_type_candidate = 'quality_issue'
                 and (
                     review_text_lc like '%wear out%'
                     or review_text_lc like '%wore out%'
                     or review_text_lc like '%not durable%'
                     or review_text_lc like '%lasted % day%'
                     or review_text_lc like '%lasted % week%'
                     or review_text_lc like '%lasted % month%'
                 )
            then 'durability_issue'

            when complaint_type_candidate = 'wrong_item'
                 and (
                     review_text_lc like '%wrong item%'
                     or review_text_lc like '%wrong product%'
                     or review_text_lc like '%different item%'
                     or review_text_lc like '%different product%'
                     or review_text_lc like '%incorrect item%'
                     or review_text_lc like '%received the wrong%'
                     or review_text_lc like '%sent the wrong%'
                 )
            then 'wrong_item_received'

            else 'general_' || complaint_type_candidate
        end as complaint_subtype

    from prepared
),

high_confidence as (
    select *
    from gated
    where operational_complaint_flag = 1
      and metadata_domain_match_flag = 1
      and narrative_content_noise_flag = 0
),

category_thresholds as (
    select
        category,
        percentile_cont(0.5) within group (order by review_length) as median_review_length
    from high_confidence
    group by category
),

scored as (
    select
        b.*,
        t.median_review_length,

        case when b.overall <= 2 then 1 else 0 end as severity_signal,
        case when coalesce(b.verified_flag, 0) = 1 then 1 else 0 end as verified_signal,
        case when coalesce(b.vote_count, 0) >= 1 then 1 else 0 end as community_signal,
        case when b.review_length >= t.median_review_length then 1 else 0 end as detail_signal

    from high_confidence b
    join category_thresholds t
      on b.category = t.category
),

final as (
    select
        *,
        severity_signal
      + verified_signal
      + community_signal
      + detail_signal as signal_score
    from scored
)

select
    asin,
    reviewer_id,
    reviewer_name,
    category,
    ingest_dt,
    overall,
    summary,
    review_text,
    review_text_clean,
    review_length,
    review_date,
    week_bucket,
    month_bucket,
    verified_flag,
    vote_count,
    positive_flag,
    neutral_flag,
    negative_flag,
    has_damage_defect,
    has_missing_parts,
    has_delivery_issue,
    has_quality_issue,
    has_wrong_item,
    issue_hint_keywords,
    complaint_type_candidate,
    complaint_subtype,
    is_low_rating_review,
    title,
    brand,
    price,
    main_cat,
    sub_category,
    root_category,
    leaf_category,
    category_depth,
    category_path,
    category_list,
    operational_complaint_flag,
    metadata_domain_match_flag,
    narrative_content_noise_flag,
    severity_signal,
    verified_signal,
    community_signal,
    detail_signal,
    signal_score,
    source_file,
    load_ts
from final
where signal_score >= 3