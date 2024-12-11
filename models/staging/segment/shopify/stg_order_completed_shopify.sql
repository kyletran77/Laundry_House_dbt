{% set source_table = source('shopify', 'order_completed') %}

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

SELECT
-- user properties
    LOWER(user_id) as user_id,
    checkout_id,
    order_id,

    timestamp as event_timestamp,

    currency,
    order_number,
    products,
    CAST(discount AS FLOAT64) AS discount,
    CAST(shipping AS FLOAT64) AS shipping,
    CAST(tax AS FLOAT64) AS tax,
    CAST(total AS FLOAT64) AS total,
    (CAST(total AS FLOAT64) - CAST(tax AS FLOAT64 )) AS net_total,


from base_source
