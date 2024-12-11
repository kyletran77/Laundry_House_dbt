select
    cast(id as STRING) as campaign_id,
    name as campaign_name,

from {{ source('google_ads', 'campaigns_view') }}
where adwords_customer_id in unnest({{ var('google_ads_customer_ids') }})






