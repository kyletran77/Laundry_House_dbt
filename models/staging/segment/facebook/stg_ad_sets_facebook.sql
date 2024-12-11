{% set ad_table = source('facebook_ads', 'ad_sets_view')%}

select
cast(id as STRING) as ad_group_id,
name as ad_group_name,


cast(campaign_id as STRING) as campaign_id,



from {{ad_table}}

 WHERE account_id IN UNNEST ({{ var('facebook_ads_account_ids') }})