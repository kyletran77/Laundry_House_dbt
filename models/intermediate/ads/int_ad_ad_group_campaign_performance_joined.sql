with
    joined_ad_tables as (

        -- GOOGLE
        select
            date,
            "google" as source,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
             ad_id,
            ad_name,
            clicks,
            impressions,
            spend
        from {{ ref("int_ad_ad_group_campaign_performance_google") }}

        union all
        -- PINTEREST
        select
            date,
            "pinterest" as source,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            ad_id,
            ad_name,
            clicks,
            impressions,
            spend
        from {{ ref("stg_ad_group_performance_pinterest") }}

        union all

        select
            date,
            "facebook" as source,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            ad_id,
            ad_name,
            clicks,
            impressions,
            spend
        from {{ ref("int_ad_ad_group_campaign_performance_facebook") }}
    ),


joined_with_most_recent_name_values as (

    SELECT 
        * except(campaign_name,ad_name, ad_group_name),
        FIRST_VALUE(campaign_name) OVER (PARTITION BY source, campaign_id ORDER BY date DESC) AS campaign_name,
        FIRST_VALUE(ad_group_name) OVER (PARTITION BY source, ad_group_id ORDER BY date DESC) AS ad_group_name,
        FIRST_VALUE(ad_name) OVER (PARTITION BY source, ad_id ORDER BY date DESC) AS ad_name
    FROM joined_ad_tables
)

select *

from joined_with_most_recent_name_values

order by 2, 1 desc
