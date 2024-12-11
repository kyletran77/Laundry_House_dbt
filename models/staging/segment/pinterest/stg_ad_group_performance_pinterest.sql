{% set ad_group_performance_table = source("pinterest_ads", "ad_group_report") %}

select
DATE(date) as date,

campaign_name,
cast(campaign_id as STRING) campaign_id,

ad_group_name,
cast(ad_group_id as STRING) ad_group_id,

ad_group_name as ad_name,
cast(ad_group_id as STRING) ad_id,

SUM(spend_in_micro_dollar / 1000000) AS spend,

SUM(cast(clickthrough_1 as FLOAT64)) as clicks,
SUM(cast(impression_1 as FLOAT64)) as impressions,
SUM(cast(outbound_click_1 as FLOAT64)) as outbound_clicks,

from {{ad_group_performance_table}}


group by 1 , 2, 3, 4 , 5, 6,7


