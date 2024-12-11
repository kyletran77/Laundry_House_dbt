with
    weighted_conversion_table_by_date as (
        select
            DATE(conversion_timestamp) AS conversion_date,
            conversion_event,
            conversion_type,
            conversion_detail,

            channel_source,
            channel_medium,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            utm_id,
            AVG(days_to_convert) AS days_to_convert,
            AVG(sessions_to_convert) AS sessions_to_convert,
            SUM(conversion_count) AS conversion_count,
            SUM(conversion_amount) AS conversion_amount,
            SUM(net_conversion_amount) AS net_conversion_amount
            

        from {{ ref("mart_conversions_first_touch") }}
        where utm_medium = "cpc" or utm_medium = "paid"
        
        group by 1, 2, 3, 4, 5, 6, 7,8, 9, 10 ,11 , 12
    ),

    ad_perfomance_date_spine as (
        select
            date,
            source,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            ad_id,
            ad_name,
            clicks,
            impressions,
            spend
        from {{ ref("int_ad_ad_group_campaign_performance_joined_date_spine") }}
    ),



joined as (
SELECT 
    COALESCE(conv.conversion_date, adpf.date) AS date,
    COALESCE(conv.channel_source, adpf.source) AS source,
    COALESCE(conv.utm_id, adpf.campaign_id, 'N/A') AS campaign_id,
    COALESCE(conv.utm_content, adpf.ad_group_id, 'N/A') AS ad_group_id,
    COALESCE(conv.utm_term, adpf.ad_id, 'N/A') AS ad_id,

    COALESCE(adpf.campaign_name, 'N/A') AS campaign_name,
    COALESCE(adpf.ad_group_name, 'N/A') AS ad_group_name,
    COALESCE(adpf.ad_name, 'N/A') AS ad_name,

    COALESCE(conv.conversion_event, 'ad_spend') AS conversion_event,
    COALESCE(conv.conversion_type, 'ad_spend') AS conversion_type,
    COALESCE(conv.conversion_detail, 'ad_spend') AS conversion_detail,

   CASE
        WHEN conv.utm_medium = 'cpc' OR conv.utm_medium = 'paid' THEN true
        ELSE false
    END AS is_paid_conversion,


    COALESCE(SUM(conv.conversion_amount), 0) AS conversion_amount,
    COALESCE(SUM(conv.net_conversion_amount), 0) AS net_conversion_amount,

    COALESCE(SUM(conv.conversion_count), 0) AS conversion_count,
    COALESCE(SUM(adpf.spend), 0) AS ad_spend,
    COALESCE(SUM(adpf.clicks), 0) AS ad_clicks,
    COALESCE(SUM(adpf.impressions), 0) AS ad_impressions,


FROM 
    weighted_conversion_table_by_date conv
FULL OUTER JOIN 
    ad_perfomance_date_spine adpf 
    ON conv.conversion_date = adpf.date AND
    conv.utm_source = adpf.source AND
    conv.utm_id = adpf.campaign_id AND
    conv.utm_content = adpf.ad_group_id AND
    conv.utm_term = adpf.ad_id
GROUP BY 
       
    COALESCE(conv.conversion_date, adpf.date),
    COALESCE(conv.channel_source, adpf.source),
    COALESCE(conv.utm_id, adpf.campaign_id, 'N/A'),
    COALESCE(conv.utm_content, adpf.ad_group_id, 'N/A'),
    COALESCE(conv.utm_term, adpf.ad_id, 'N/A'),
    COALESCE(adpf.campaign_name, 'N/A'),
    COALESCE(adpf.ad_group_name, 'N/A'),
    COALESCE(adpf.ad_name, 'N/A'),
    COALESCE(conv.conversion_event, 'ad_spend'),
    COALESCE(conv.conversion_type, 'ad_spend'),
    COALESCE(conv.conversion_detail, 'ad_spend'),    
    is_paid_conversion    
)


select * from joined
