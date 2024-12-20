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
                cast(first_touch_session_id as string) as first_touch_session_id
            from {{ ref('mart_conversions_first_touch') }}
            where conversion_date > timestamp('2024-12-04')
            order by rand()
            limit 10
        ),

        -- Check against sessions
        validation_results as (
            select
                c.email,
                c.conversion_type,
                c.conversion_date,
                -- Does first touch session exist in mart_sessions?
                exists (
                    select 1 
                    from {{ ref('mart_sessions') }} s
                    where cast(s.session_id as string) = c.first_touch_session_id
                ) as session_exists,
                
                -- Does attribution match?
                exists (
                    select 1 
                    from {{ ref('mart_sessions') }} s
                    where cast(s.session_id as string) = c.first_touch_session_id
                    and (
                        cast(s.utm_source as string) = c.first_touch_source
                        or cast(s.referrer_source as string) = c.first_touch_source
                        or cast(s.channel_source as string) = c.first_touch_source
                    )
                    and (
                        cast(s.utm_medium as string) = c.first_touch_medium
                        or cast(s.referrer_medium as string) = c.first_touch_medium
                        or cast(s.channel_medium as string) = c.first_touch_medium
                    )
                    and (cast(s.utm_campaign as string) = c.first_touch_campaign)
                ) as attribution_matches,
                
                -- Is it really the first session?
                not exists (
                    select 1 
                    from {{ ref('mart_sessions') }} s
                    where s.blended_user_id = c.email
                    and s.session_start_timestamp < c.first_touch_timestamp
                ) as is_truly_first_touch,

                -- Raw data for verification
                c.first_touch_source,
                c.first_touch_medium,
                c.first_touch_campaign,
                c.first_touch_timestamp,
                c.first_touch_session_id
            from sample_conversions c
        )

        select * from validation_results
    {% endset %}

    {% set results = run_query(validation_query) %}
    
    {% if execute %}
        {{ log("=== Attribution Mapping Validation ===", info=True) }}
        {{ log("", info=True) }}
        {% for row in results.rows %}
            {{ log("User: " ~ row[0], info=True) }}
            {{ log("Conversion: " ~ row[1] ~ " on " ~ row[2], info=True) }}
            {{ log("VALIDATION RESULTS:", info=True) }}
            {{ log("  Session Exists: " ~ row[3], info=True) }}
            {{ log("  Attribution Matches: " ~ row[4], info=True) }}
            {{ log("  Is Truly First Touch: " ~ row[5], info=True) }}
            {{ log("", info=True) }}
            {{ log("DEBUG DATA:", info=True) }}
            {{ log("  Source: " ~ row[6], info=True) }}
            {{ log("  Medium: " ~ row[7], info=True) }}
            {{ log("  Campaign: " ~ row[8], info=True) }}
            {{ log("  Timestamp: " ~ row[9], info=True) }}
            {{ log("  Session ID: " ~ row[10], info=True) }}
            {{ log("----------------------------------------", info=True) }}
            {{ log("", info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}