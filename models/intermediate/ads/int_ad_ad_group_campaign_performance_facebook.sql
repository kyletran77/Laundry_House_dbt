with
    campaigns_and_adset as (

        select distinct campaign_id, campaign_name, ad_group_id, ad_group_name

        from {{ ref("stg_campaigns_facebook") }}
        left join
            (
                select ad_group_id, ad_group_name, campaign_id
                from {{ ref("stg_ad_sets_facebook") }}

            ) using (campaign_id)
    ),

    ads_with_campaigns_and_adsets as (
        select distinct cas.*, ad_id, ad_name
        from {{ ref("stg_ads_facebook") }}
        left join (select * from campaigns_and_adset) as cas using (ad_group_id)
    ),

    final_ad_performance_table_enriched as (
        select
            date,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            ad_id,
            ad_name,
            clicks,
            impressions,
            spend

        from {{ ref("stg_ad_performance_facebook") }}
        left join ads_with_campaigns_and_adsets using (ad_id)
    )

select *
from final_ad_performance_table_enriched
