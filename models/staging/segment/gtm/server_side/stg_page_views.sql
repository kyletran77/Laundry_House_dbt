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
    context_geo_city as geo_city,
    context_geo_country as geo_country,
    context_geo_region as geo_region,
    context_ip as ip_address,
    context_screen_screen_resolution as screen_screen_resolution,
    context_screen_viewport_size as screen_viewport_size,
    context_user_agent as user_agent,
    -- context_user_agent_data_brands as user_agent_brands,
    context_user_agent_data_browser_brand as device_browser_brand,
    -- context_user_agent_data_browser_version as device_browser_version,
    -- context_user_agent_data_engine_name as device_engine_name,
    -- context_user_agent_data_engine_version as device_engine_version,
    context_user_agent_data_mobile as device_is_mobile,
    context_user_agent_data_platform as device_platform,

    -- page properties
    context_page_path as page_path,
     concat(context_page_path , {{ dbt_utils.get_url_host("context_page_url") }}) as page_path_host,
   
    context_page_referrer as page_referrer,
    replace({{ dbt_utils.get_url_host("referrer") }}, 'www.', '') as page_referrer_host,
    context_page_search as page_query,
    context_page_title as page_title,
    context_page_url as page_url,
    {{ dbt_utils.get_url_host("context_page_url") }} as page_host,
    -- marketing related fields 
    {{
        escape_temporary_utm_adjustment(
            "context_campaign_source",
            "context_campaign_medium",
            "context_campaign_campaign",
            "context_campaign_term",
            "context_campaign_content",
            "context_page_url",
        )
    }},

    {{ dbt_utils.get_url_parameter("context_page_url", "utm_id") }} as utm_id,

    {{ get_all_click_ids("context_page_url") }},

    -- event properties
    sent_at as page_view_timestamp,
    id as page_view_id,

from base_source
),
referrer_mapping as (

    select * from {{ ref('referrer_mapping') }}

),
 page_views_mapped_referrer as (

    select
        page_views.*,
        lower(referrer_mapping.medium) as referrer_medium,
        lower(referrer_mapping.source) as referrer_source

    from page_views as page_views

    left join referrer_mapping on page_views.page_referrer_host = referrer_mapping.host

),

mapped_refferer_with_channel_group as (
    select 

    distinct
    
    page_views_mapped_referrer.*,
    coalesce(
        utm_source,
        CASE
            WHEN referrer_source IS NOT NULL 
                 AND NOT referrer_source IN UNNEST({{ var('domain_hosts') | tojson | safe }})
                 AND NOT referrer_source IN UNNEST({{ var('referral_exclusions') | tojson | safe }})
            THEN referrer_source
            WHEN page_referrer_host IS NOT NULL 
                 AND NOT page_referrer_host IN UNNEST({{ var('domain_hosts') | tojson | safe }})
                 AND NOT page_referrer_host IN UNNEST({{ var('referral_exclusions') | tojson | safe }})
            THEN page_referrer_host
            ELSE '(direct/none)'
        END
    ) as channel_source,
    coalesce(utm_medium, referrer_medium) as channel_medium
    from page_views_mapped_referrer
)


select * from mapped_refferer_with_channel_group 

union all 

select * from {{ref('stg_manual_imports')}}


