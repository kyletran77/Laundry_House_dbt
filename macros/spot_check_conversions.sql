{% macro spot_check_conversions() %}
    {% set validation_query %}
        -- Get 10 diverse users (mix of leads and sales)
        with sample_users as (
            select distinct email
            from {{ ref('mart_conversions_first_touch') }}
            where email is not null
            and conversion_date > timestamp('2024-12-04')
            order by rand()
            limit 10
        ),

        -- Get all conversions for these users
        user_conversions as (
            select 
                email,
                conversion_type,
                conversion_date,
                first_touch_source,
                first_touch_medium,
                first_touch_campaign,
                first_touch_timestamp,
                revenue
            from {{ ref('mart_conversions_first_touch') }}
            where email in (select email from sample_users)
        ),

        -- Get ALL sessions for these users
        user_sessions as (
            select 
                s.blended_user_id as email,
                s.session_id,
                s.session_start_timestamp,
                cast(s.utm_source as string) as utm_source,
                cast(s.utm_medium as string) as utm_medium,
                cast(s.utm_campaign as string) as utm_campaign,
                cast(s.referrer_source as string) as referrer_source,
                cast(s.referrer_medium as string) as referrer_medium,
                cast(s.page_referrer as string) as page_referrer
            from {{ ref('mart_sessions') }} s
            where s.blended_user_id in (select email from sample_users)
            and s.session_start_timestamp > timestamp('2024-12-04')
        )

        -- Output everything in chronological order
        select
            'CONVERSION' as record_type,
            c.email,
            cast(c.conversion_date as string) as event_timestamp,
            c.conversion_type as event_detail,
            concat(
                'First Touch: ', coalesce(cast(c.first_touch_source as string), 'null'), 
                ' / ', coalesce(cast(c.first_touch_medium as string), 'null'),
                ' / ', coalesce(cast(c.first_touch_campaign as string), 'null'),
                ' | Revenue: ', cast(c.revenue as string)
            ) as additional_info
        from user_conversions c

        union all

        select
            'SESSION' as record_type,
            s.email,
            cast(s.session_start_timestamp as string) as event_timestamp,
            s.session_id as event_detail,
            concat(
                'UTM: ', coalesce(s.utm_source, 'null'), 
                ' / ', coalesce(s.utm_medium, 'null'),
                ' / ', coalesce(s.utm_campaign, 'null'),
                ' | Referrer: ', coalesce(s.referrer_source, 'null'),
                ' / ', coalesce(s.referrer_medium, 'null'),
                ' | Page: ', coalesce(s.page_referrer, 'null')
            ) as additional_info
        from user_sessions s

        order by 
            email,
            event_timestamp,
            record_type desc
    {% endset %}

    {% set results = run_query(validation_query) %}
    
    {% if execute %}
        {{ log("=== Deep Dive: 10 Random User Journeys ===", info=True) }}
        {{ log("", info=True) }}
        {% set current_user = none %}
        {% for row in results.rows %}
            {% if row[1] != current_user %}
                {% set current_user = row[1] %}
                {{ log("", info=True) }}
                {{ log("====================================", info=True) }}
                {{ log("=== User: " ~ row[1] ~ " ===", info=True) }}
                {{ log("====================================", info=True) }}
            {% endif %}
            
            {% if row[0] == 'CONVERSION' %}
                {{ log("[CONVERSION] - " ~ row[3], info=True) }}
                {{ log("  Time: " ~ row[2], info=True) }}
                {{ log("  " ~ row[4], info=True) }}
                {{ log("", info=True) }}
            {% else %}
                {{ log("[SESSION] - " ~ row[3], info=True) }}
                {{ log("  Time: " ~ row[2], info=True) }}
                {{ log("  " ~ row[4], info=True) }}
                {{ log("", info=True) }}
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}