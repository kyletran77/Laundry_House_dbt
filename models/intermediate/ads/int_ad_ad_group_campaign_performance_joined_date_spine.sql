-- this table joines all possible source, campaign, adset, ad for all add sources, to a datespine so that we get an entry for the combinations for every single day.

WITH distinct_combinations AS (
    SELECT DISTINCT 
        source, 
        campaign_id, 
        campaign_name, 
        ad_group_id, 
        ad_group_name, 
        ad_id, 
        ad_name 
    FROM {{ ref('int_ad_ad_group_campaign_performance_joined') }}
),

    date_spine_days as (
        select date(date_day) as date
        from

            (
                {{
                    dbt_utils.date_spine(
                        datepart="day",
                        start_date="(SELECT MIN(date) FROM (SELECT MIN(date) AS date FROM "
                        + ref("int_ad_ad_group_campaign_performance_joined")
                        | string
                        + " UNION ALL SELECT MIN(CAST(event_timestamp AS DATE)) AS date FROM "
                        + ref("int_conversions")
                        | string + ") AS min_dates)",
                        end_date="(SELECT MAX(date) + INTERVAL 1 DAY FROM (SELECT MAX(date) AS date FROM "
                        + ref("int_ad_ad_group_campaign_performance_joined")
                        | string
                        + " UNION ALL SELECT MAX(CAST(event_timestamp AS DATE)) AS date FROM "
                        + ref("int_conversions")
                        | string + ") AS max_dates)"

                    )
                }}

            )
        order by 1 desc
    ),

date_spine_with_combinations AS (
    SELECT 
        d.date, 
        c.source, 
        c.campaign_id, 
        c.campaign_name, 
        c.ad_group_id, 
        c.ad_group_name, 
        c.ad_id, 
        c.ad_name
    FROM date_spine_days d
    CROSS JOIN distinct_combinations c
)

SELECT 
    cj.date, 
    cj.source, 
    cj.campaign_id, 
    cj.campaign_name, 
    cj.ad_group_id, 
    cj.ad_group_name, 
    cj.ad_id, 
    cj.ad_name,
    COALESCE(ap.spend, 0) AS spend,
    COALESCE(ap.clicks, 0) AS clicks,
    COALESCE(ap.impressions, 0) AS impressions
FROM 
    date_spine_with_combinations cj
LEFT JOIN 
   {{ ref('int_ad_ad_group_campaign_performance_joined') }}   ap 
    ON cj.date = ap.date AND 
       cj.source = ap.source AND 
       cj.campaign_id = ap.campaign_id AND 
       cj.ad_group_id = ap.ad_group_id AND 
       COALESCE(cj.ad_id, "") = COALESCE(ap.ad_id, "")





