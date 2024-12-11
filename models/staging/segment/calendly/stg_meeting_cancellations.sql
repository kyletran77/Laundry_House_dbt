{% set source_table = source('calendly', 'meeting_cancelled') %}

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
  -- Other Timestamps
  timestamp as event_timestamp,

--id's
  user_id,

-- meeting_invitee_id serves as the main meeting id
  invitee_uri as meeting_id, 

  -- Cancellation Information
  canceled_by,
  canceler_type,
  cancellation_reason,

FROM base_source
