{% set source_table = source("gtm_server_side", "tracks") %}

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
    )


select distinct
  timestamp as event_timestamp,
  user_id,
  anonymous_id,
  id as track_id,
  event as event_name
from base_source