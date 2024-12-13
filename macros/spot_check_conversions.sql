{% macro spot_check_conversions() %}
    {% set validation_query %}
        -- Get 10 random conversions
        with sample_conversions as (
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
            where email is not null
            order by rand()
            limit 10
        ),

        -- Get all sessions for these users
        user_sessions as (
            select 
                s.blended_user_id as email,
                s.session_start_timestamp,
                s.utm_source,
                s.utm_medium,
                s.utm_campaign,
                s.page_url,
                s.page_referrer
            from {{ ref('mart_sessions') }} s
            inner join sample_conversions c 
                on s.blended_user_id = c.email
            order by 
                s.blended_user_id,
                s.session_start_timestamp
        )

        -- Output detailed user journeys
        select
            'CONVERSION' as record_type,
            c.email,
            c.conversion_type,
            cast(c.conversion_date as string) as event_date,
            c.first_touch_source as source,
            c.first_touch_medium as medium,
            c.first_touch_campaign as campaign,
            cast(c.revenue as string) as additional_info
        from sample_conversions c

        union all

        select
            'SESSION' as record_type,
            s.email,
            cast(s.session_start_timestamp as string) as conversion_type,
            cast(s.session_start_timestamp as string) as event_date,
            s.utm_source as source,
            s.utm_medium as medium,
            s.utm_campaign as campaign,
            s.page_url as additional_info
        from user_sessions s

        order by 
            email,
            event_date
    {% endset %}

    {% set results = run_query(validation_query) %}
    
    {% if execute %}
        {{ log("=== Spot Check: 10 Random User Journeys ===", info=True) }}
        {{ log("", info=True) }}
        {% set current_user = none %}
        {% for row in results.rows %}
            {% if row[1] != current_user %}
                {% set current_user = row[1] %}
                {{ log("", info=True) }}
                {{ log("=== User: " ~ row[1] ~ " ===", info=True) }}
            {% endif %}
            
            {% if row[0] == 'CONVERSION' %}
                {{ log("CONVERSION - " ~ row[2] ~ " on " ~ row[3], info=True) }}
                {{ log("Attribution: " ~ row[4] ~ " / " ~ row[5] ~ " / " ~ row[6], info=True) }}
                {{ log("Revenue: " ~ row[7], info=True) }}
            {% else %}
                {{ log("SESSION on " ~ row[3], info=True) }}
                {{ log("Source/Medium: " ~ row[4] ~ " / " ~ row[5], info=True) }}
                {{ log("URL: " ~ row[7], info=True) }}
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}