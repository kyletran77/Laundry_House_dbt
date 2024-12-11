select
    DATE(date_start) as date,

    cast(campaign_id as STRING) as campaign_id,

    SUM(cast(clicks as FLOAT64)) as clicks,
    SUM(cast(impressions as FLOAT64)) as impressions,

    SUM(cast(cost as FLOAT64) / 1000000) as spend,

from {{ source('google_ads', 'campaign_performance_reports_view') }}
where adwords_customer_id in unnest({{ var('google_ads_customer_ids') }})

group by 1, 2






