{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_timestamp',
    partition_by = {'field': 'session_start_timestamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}


{% set window_clause_first_and_last_pages = "         partition by session_id          order by timestamp asc          rows between unbounded preceding and unbounded following     " %}

{% set first_values = {
    "geo_city": "geo_city",
    "geo_region": "geo_region",
    "geo_country": "geo_country",
    "page_referrer": "page_referrer",
    "page_referrer_host": "page_referrer_host",
    "referrer_medium": "referrer_medium",
    "referrer_source": "referrer_source",
    "channel_medium": "channel_medium",
    "channel_source": "channel_source",
    "device_browser_brand": "device_browser_brand",
    "device_is_mobile": "device_is_mobile",
    "device_platform": "device_platform",
    "utm_content": "utm_content",
    "utm_medium": "utm_medium",
    "utm_source": "utm_source",
    "utm_campaign": "utm_campaign",
    "utm_term": "utm_term",
    "utm_id": "utm_id",
    "page_url": "first_page_url",
    "page_title": "first_page_title",
    "page_host": "first_page_host",
    "page_query": "first_page_query",
    "page_path": "first_page_path",
    "page_path_host": "first_page_path_host"
} %}


{% set last_values = {
    "page_url": "last_page_url",
    "page_referrer": "last_page_referrer",
    "page_title": "last_page_title",
    "page_query": "last_page_query",
    "page_path": "last_page_path",
    "page_host": "last_page_host",
    "page_path_host": "last_page_path_host"
} %}


  with  first_and_last_page_values_for_session as (
        select distinct
            session_id,         
            {% for (key, value) in first_values.items() %}
                first_value({{ key }}) over ({{ window_clause_first_and_last_pages }}) as {{ value }},
            {% endfor %}

            {% for (key, value) in last_values.items() %}
                last_value({{ key }}) over ({{ window_clause_first_and_last_pages }}) as {{ value }}
                {% if not loop.last %},{% endif %}
            {% endfor %}

        from {{ref('int_sessionized_page_views')}}
    ),

 output as (

        select
            sessionized_events.session_id,
            sessionized_events.blended_user_id,
            sessionized_events.anonymous_id,
             sessionized_events.session_index as session_number,
           
            first_and_last_page_values_for_session.* except(session_id) ,
            min(sessionized_events.timestamp) as session_start_timestamp,
            max(sessionized_events.timestamp) as session_end_timestamp,
            -- stats about the session
            count(sessionized_events.page_view_id) as total_pages,
            timestamp_diff(
                max(sessionized_events.timestamp),
                min(sessionized_events.timestamp),
                millisecond
            )
            / 1000 as duration_in_seconds,
            -- repeated array of records
            array_agg(
                struct(
                    sessionized_events.timestamp,
                    struct(
                        sessionized_page_views.timestamp,
                        sessionized_page_views.page_view_id,
                        sessionized_page_views.page_url,
                        sessionized_page_views.page_referrer,
                        sessionized_page_views.page_title,
                        sessionized_page_views.page_query,
                        sessionized_page_views.page_path,
                        sessionized_page_views.utm_content,
                        sessionized_page_views.utm_medium,
                        sessionized_page_views.utm_source,
                        sessionized_page_views.utm_campaign,
                        sessionized_page_views.utm_term,
                        sessionized_page_views.utm_id
                    ) as page_view_records
                )
                order by sessionized_events.timestamp asc
            ) as records
        from {{ ref("int_sessionized_events") }} as sessionized_events
        left join first_and_last_page_values_for_session 
         using (session_id)

        left join
            {{ ref("int_sessionized_page_views") }} as sessionized_page_views using(page_view_id)

       {{ dbt_utils.group_by(35) }}

    )

select *




from output


