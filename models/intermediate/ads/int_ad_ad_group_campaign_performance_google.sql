with

    ad_ad_group as (
        select distinct ad_id, ad_group_id, ad_group_name, campaign_id
        from {{ ref("stg_ads_google") }}
        left join {{ ref("stg_ad_groups_google") }} using (ad_group_id)
    ),

    ad_ad_group_campaign as (
        select distinct campaign_name, campaign_id, ad_group_name, ad_group_id, ad_id
        from ad_ad_group
        left join {{ ref("stg_campaigns_google") }} using (campaign_id)

    ),

    ad_performance_enriched as (
        select
            date,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            ad_id,
            ad_id as ad_name,
            clicks,
            impressions,
            spend

        from {{ ref("stg_ad_performance_google") }}
        left join
            (
                select distinct campaign_id, campaign_name, ad_group_name, ad_group_id,
                from ad_ad_group_campaign

            ) using (ad_group_id)
    ),

    campaign_performance_without_campaigns_that_exist_in_adperformance as (
        select

            date,
            campaign_id,
            campaign_name,
            campaign_id as ad_group_id,
            campaign_name as ad_group_name,
            campaign_id as ad_id,
            campaign_name as ad_name,
            clicks,
            impressions,
            spend
        from {{ ref("stg_campaign_performance_google") }}
        left join
            (
                select distinct campaign_id, campaign_name
                from {{ ref("stg_campaigns_google") }}
            ) using (campaign_id)
        where
            campaign_id
            not in (select distinct campaign_id from ad_performance_enriched)
    )

select *
from ad_performance_enriched

union all

select *
from campaign_performance_without_campaigns_that_exist_in_adperformance
