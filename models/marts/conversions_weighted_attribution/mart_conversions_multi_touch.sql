{% set lookback_days = 60 %}

with
    conversions as (
        select
            user_id,
            event_timestamp as conversion_timestamp,
            conversion_event,
            conversion_type,
            conversion_detail,
            net_conversion_amount,

            conversion_amount
        from {{ ref("int_conversions") }}
    ),

    sessions as (
        select
            blended_user_id,
            session_start_timestamp,
            channel_source,
            channel_medium,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            utm_id,
            row_number() over (
                partition by blended_user_id order by session_start_timestamp
            ) rn

        from {{ ref("mart_sessions") }}
    ),

    conversions_with_sessions as (
        select
            c.*,
            s.* except (blended_user_id, rn),
            date_diff(
                c.conversion_timestamp,
                min(s.session_start_timestamp) over (partition by c.user_id),
                day
            ) as days_to_convert
        from conversions c
        left join
            sessions s
            on c.user_id = s.blended_user_id
            and s.session_start_timestamp <= c.conversion_timestamp
            and c.conversion_timestamp - s.session_start_timestamp
            <= interval {{ lookback_days }} day
    ),
    weighted_touchpoints as (
        select
            *,
            count(session_start_timestamp) over (
                partition by user_id, conversion_timestamp
            ) as sessions_to_convert
        from conversions_with_sessions
    )


   

select
    user_id,
    conversion_timestamp,
    conversion_event,
    conversion_type,
    conversion_detail,
    channel_source,
    channel_medium,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    utm_id,
    sessions_to_convert,
    case
        when sessions_to_convert = 0 then 0 else 1.0 / sessions_to_convert
    end as conversion_count,
    case
        when sessions_to_convert = 0 then 0 else conversion_amount / sessions_to_convert
    end as conversion_amount,
    case
        when sessions_to_convert = 0 then 0 else net_conversion_amount / sessions_to_convert
    end as net_conversion_amount

from weighted_touchpoints
