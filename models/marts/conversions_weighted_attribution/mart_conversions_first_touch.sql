-- Set a 60-day lookback window for attributing conversions to first touch
{% set lookback_days = 60 %}

with
    -- Extract relevant conversion data from the intermediate conversions table
    conversions as (
        select
            user_id,
            event_timestamp as conversion_timestamp,
            conversion_event,
            conversion_type,
            conversion_amount,
            conversion_detail,            
            net_conversion_amount
        from {{ ref("int_conversions") }}
    ),

    -- Get all user sessions with their UTM parameters and channel information
    -- Add a row number to identify first sessions per user
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
            first_page_path_host,
            row_number() over (
                partition by blended_user_id order by session_start_timestamp
            ) as rn
        from {{ ref("mart_sessions") }}
    ),

    -- Filter to only keep the first session for each user
    first_sessions as (select * from sessions where rn = 1),

    -- Join conversions with users' first sessions
    -- This implements first-touch attribution by connecting conversions
    -- to the user's very first interaction within the lookback window
    conversions_with_first_touch as (
        select
            c.*,
            fs.* except (blended_user_id, rn),
            -- Calculate how many days it took to convert after first touch
            date_diff(
                c.conversion_timestamp, fs.session_start_timestamp, day
            ) as days_to_convert,
            1 as sessions_to_convert
        from conversions c
        left join
            first_sessions fs
            on c.user_id = fs.blended_user_id
            -- Only attribute conversions that happened after the first session
            and fs.session_start_timestamp <= c.conversion_timestamp
            -- Only look back up to the specified number of days
            and c.conversion_timestamp - fs.session_start_timestamp
            <= interval {{ lookback_days }} day
    )

-- Final output combines conversion data with first-touch attribution data
-- Each conversion is attributed 100% to the first touch (hence conversion_count = 1.0)
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
    first_page_path_host,   
    days_to_convert,
    sessions_to_convert,
    1.0 as conversion_count, -- Each conversion is attributed 100% to first touch
    conversion_amount,
    net_conversion_amount  
from conversions_with_first_touch
order by conversion_timestamp desc
