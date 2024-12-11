with

    weighted_conversion_table_by_date as (
        select
            date(conversion_timestamp) as date,
            channel_source as source,
            utm_id as campaign_id,
            utm_content as ad_group_id,
            utm_term as ad_id,
            campaign_name,
            ad_group_name,
            ad_name,
            conversion_event,
            conversion_type,
            conversion_detail,
            user_id,
            conversion_amount,
            net_conversion_amount,
            conversion_count,
            0 as ad_spend,
            0 as ad_clicks,
            0 as ad_impressions,
        -- days_to_convert,
        -- sessions_to_convert,
        from {{ ref("mart_conversions_first_touch") }} conv
        left join
            (
                select
                    campaign_id,
                    ad_group_id,
                    ad_id,
                    campaign_name,
                    ad_group_name,
                    ad_name
                from {{ ref("int_ad_ad_group_campaign_performance_joined") }}

                group by 1, 2, 3, 4, 5, 6
            ) adnames

            on conv.utm_id = adnames.campaign_id
            and conv.utm_content = adnames.ad_group_id
            and conv.utm_term = adnames.ad_id

        where utm_medium = "cpc" or utm_medium = "paid"
    ),

    ad_perfomance_date_spine as (
        select
            date,
            source,
            campaign_id,
            ad_group_id,
            ad_id,
            campaign_name,
            ad_group_name,
            ad_name,
            '~ADSPEND~' as conversion_event,
            '~ADSPEND~' as conversion_type,
            '~ADSPEND~' as conversion_detail,
            'null' as user_id,
            0 as conversion_amount,
            0 as net_conversion_amount,
            0 as conversion_count,
            spend as ad_spend,
            clicks as ad_clicks,
            impressions as ad_impressions
        from {{ ref("int_ad_ad_group_campaign_performance_joined_date_spine") }}
    ),

    joined as (
        select *
        from weighted_conversion_table_by_date

        union all

        select *
        from ad_perfomance_date_spine

        order by date desc
    ),

    joined_cumulatives as (
        select
            *,
            sum(coalesce(conversion_amount, 0)) over (
                order by date rows between unbounded preceding and current row
            ) as cumulative_conversion_amount,
            sum(coalesce(net_conversion_amount, 0)) over (
                order by date rows between unbounded preceding and current row
            ) as cumulative_net_conversion_amount,
            sum(coalesce(ad_spend, 0)) over (
                order by date rows between unbounded preceding and current row
            ) as cumulative_ad_spend

        from joined
    )

select *
from joined_cumulatives

order by date desc
