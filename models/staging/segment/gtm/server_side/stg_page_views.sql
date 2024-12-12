{% set source_table = source("gtm_server_side", "pages") %}

with
    base_source as (
        select * except (__row_number)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id order by loaded_at desc
                    ) as __row_number
                from {{ source_table }}
                {% if var("event_lookback_window") is not none %}
                    where
                        _partitiontime between timestamp_trunc(
                            timestamp_micros(
                                unix_micros(current_timestamp())
                                - {{ var("event_lookback_window") }}
                            ),
                            day,
                            'UTC'
                        ) and timestamp_trunc(current_timestamp(), day, 'UTC')
                {% endif %}
            )
        where __row_number = 1
    ),

page_views as (
    select
        -- user properties
        anonymous_id,
        user_id,

        -- contextual properties
        context_edge_city as geo_city,
        context_edge_country as geo_country,
        context_edge_region as geo_region,
        context_ip as ip_address,
        context_user_agent as user_agent,
        context_user_agent_data_mobile as device_is_mobile,
        context_user_agent_data_platform as device_platform,

        -- page properties
        context_page_path as page_path,
        concat(context_page_path, {{ dbt_utils.get_url_host("context_page_url") }}) as page_path_host,
        context_page_referrer as page_referrer,
        replace({{ dbt_utils.get_url_host("referrer") }}, 'www.', '') as page_referrer_host,
        context_page_search as page_query,
        context_page_title as page_title,
        context_page_url as page_url,
        {{ dbt_utils.get_url_host("context_page_url") }} as page_host,

        -- marketing related fields 
        context_campaign_source as utm_source,
        context_campaign_medium as utm_medium,
        context_campaign_name as utm_campaign,
        context_campaign_term as utm_term,
        context_campaign_content as utm_content,
        {{ dbt_utils.get_url_parameter("context_page_url", "utm_id") }} as utm_id,

        -- event properties
        sent_at as page_view_timestamp,
        id as page_view_id,

        -- default channel values
        '(direct)' as channel_source,
        '(none)' as channel_medium,
        null as referrer_source,
        null as referrer_medium

    from base_source
)

select * from page_views