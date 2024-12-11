select
    cast(id as STRING) as ad_group_id,
    name as ad_group_name,
    cast(campaign_id as STRING) as campaign_id,

from {{ source('google_ads', 'ad_groups_view') }}
where adwords_customer_id in unnest({{ var('google_ads_customer_ids') }})






