select
    SPLIT(cast(id as STRING), '::')[OFFSET(1)]  as ad_id,
    cast(ad_group_id as STRING) as ad_group_id,

from {{ source('google_ads', 'ads_view') }}
where adwords_customer_id in unnest({{ var('google_ads_customer_ids') }})






