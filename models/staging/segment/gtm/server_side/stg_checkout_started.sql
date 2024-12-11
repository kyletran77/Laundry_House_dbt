-- This is a staging model for checkout events
{% set source_table = source('gtm_server_side', 'checkout_started') %}

with base_source as (
    -- This CTE deduplicates events by keeping only the latest version of each event
    SELECT * EXCEPT (__row_number) FROM (
        SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number 
        FROM {{source_table}}
        -- Optional time window filter
        {% if var('event_lookback_window') is not none %}
            WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - {{var('event_lookback_window')}}), DAY, 'UTC')
                AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
        {% endif %}
    )
    WHERE __row_number = 1  -- Keep only the latest version
)

select
anonymous_id,    -- Identifier for anonymous users
user_id,         -- Identifier for logged-in users
timestamp as event_timestamp,  -- When the checkout started

-- form properties (commented out in your version)

from base_source
