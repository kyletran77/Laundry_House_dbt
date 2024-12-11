with page_views_w_sessions as (
    select
        sessionized_events.session_id,
        sessionized_events.timestamp as page_view_timestamp,
        sessionized_events.blended_user_id,
        sessionized_events.anonymous_id,
        sessionized_events.session_index,
        sessionized_events.page_view_id,
        -- page information
        page_views.page_url,
        page_views.page_referrer,
        page_views.page_title,
        page_views.page_query,
        page_views.page_path,
        page_views.page_host, 
        page_views.page_referrer_host,  
        page_views.page_path_host,
        -- marketing related fields
        page_views.utm_content,
        page_views.utm_medium,
        page_views.utm_source,
        page_views.utm_campaign,
        page_views.utm_term,
        page_views.utm_id,
        -- channel grouping
        page_views.channel_source,
        page_views.channel_medium,
        page_views.referrer_source,
        page_views.referrer_medium,
        -- device information
        page_views.device_browser_brand,
        page_views.device_is_mobile,
        page_views.device_platform,
        -- geo information
        page_views.geo_city,
        page_views.geo_country,
        page_views.geo_region  
    from 
        {{ref('int_sessionized_events')}} as sessionized_events
        left join {{ref('stg_page_views')}} as page_views
            using(page_view_id)
)

select * from page_views_w_sessions



