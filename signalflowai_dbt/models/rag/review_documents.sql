select
    row_number() over (
        order by review_date, asin, reviewer_id
    ) as doc_id,

    asin,
    reviewer_id,
    category,
    review_date,
    week_bucket,
    month_bucket,

    brand,
    title,

    complaint_type_candidate as complaint_type,
    complaint_subtype,
    signal_score,

    summary,
    review_text_clean as review_text,

    concat_ws(
        ' ',
        'Product:', coalesce(title, ''),
        'Brand:', coalesce(brand, ''),
        'Category:', coalesce(category, ''),
        'Complaint Type:', coalesce(complaint_type_candidate, ''),
        'Complaint Subtype:', coalesce(complaint_subtype, ''),
        'Summary:', coalesce(summary, ''),
        'Review:', coalesce(review_text_clean, '')
    ) as retrieval_text

from {{ ref('review_enriched_complaints') }}