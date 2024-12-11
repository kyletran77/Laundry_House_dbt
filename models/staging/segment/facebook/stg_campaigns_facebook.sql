{% set ad_table = source('facebook_ads', 'campaigns_view')%}

select
cast(id as STRING) as campaign_id,
name as campaign_name,






from {{ad_table}}

 WHERE account_id IN UNNEST ({{ var('facebook_ads_account_ids') }})