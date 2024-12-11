{% set ad_performance_table = source('facebook_ads', 'insights_view')%}

select
DATE(date_start) as date,

cast(ad_id as STRING) as ad_id,

-- link clicks. post likes, comments, shares clicks to facebook page, and lin
SUM(cast(link_clicks as FLOAT64)) as clicks,

-- cast(unique_clicks as FLOAT64) as unique_clicks,
-- cast(unique_clicks as FLOAT64) as unique_clicks,

SUM(impressions) as impressions,
-- unique_impressions

-- Link clicks are the number of clicks on links to select destinations or experiences, on or off Meta technologies.
SUM(cast(link_clicks as FLOAT64)) as link_clicks,

SUM(spend) as spend,

from {{ad_performance_table}}


WHERE
    ad_id IN (
        SELECT id as ad_id 
        FROM {{ source('facebook_ads', 'ads_view') }} 
        WHERE account_id IN UNNEST({{ var('facebook_ads_account_ids') }})
    )

group by 1, 2




