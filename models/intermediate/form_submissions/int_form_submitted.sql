{% set server_side_gtm_form_model = ref('stg_form_submitted_gtm_server_side') %}

{% set form_provider_models = [
    ref('stg_form_submitted_gohighlevel')
] %}

{% set form_column_names = [
    'user_id',
    'event_timestamp',
    'form_name',
    'submission_source'
] %}

WITH form_provider_form_names AS (
    SELECT 
        DISTINCT form_name
    FROM (
        {% for model in form_provider_models %}
            SELECT form_name FROM {{ model }}
            {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
    )
),

gtm_server_side_excluded AS (
    SELECT 
        {{ form_column_names | join(', ') }}
    FROM 
        {{ server_side_gtm_form_model }}
    WHERE 
        form_name NOT IN (SELECT form_name FROM form_provider_form_names)
),

combined_data AS (
    SELECT 
        {{ form_column_names | join(', ') }}
    FROM 
        gtm_server_side_excluded

    UNION ALL

    {% for model in form_provider_models %}
        SELECT 
            {{ form_column_names | join(', ') }}
        FROM 
            {{ model }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

combined_with_historical as (
    SELECT
        *
    FROM
        combined_data


        union ALL
        

        select user_id as user_id, page_view_timestamp as event_timestamp, 'Slaapkamers: E-Book Ontvangen Telefoon' as form_name, 'ghl' as  submission_source
        from {{ref('stg_manual_imports')}}
        where user_id not in (
            select user_id from combined_data
        ) and user_id not in (
            select user_id from {{ref('int_meeting_bookings')}}
        )
)

select * except(event_timestamp), min(event_timestamp) as event_timestamp from combined_with_historical

group by 1,2,3