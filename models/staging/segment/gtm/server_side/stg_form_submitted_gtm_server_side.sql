{% set source_table = source('gtm_server_side', 'form_submitted') %}

with base_source as (
    SELECT * EXCEPT (__row_number) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number FROM {{source_table}}
            {% if var('event_lookback_window') is not none %}
            WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - {{var('event_lookback_window')}}), DAY, 'UTC')
                AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
            {% endif %}
            )
    WHERE __row_number = 1
)

select
user_id,

timestamp as event_timestamp,


-- form properties
form_name,
'gtm_server_side' as submission_source


from 
base_source
where form_name is not null