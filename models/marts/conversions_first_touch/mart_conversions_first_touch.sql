{{ config(
    materialized = 'incremental',
    unique_key = 'conversion_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['email', 'conversion_type']
) }}

with first_touch_sessions as (
    select
        blended_user_id,
        anonymous_id,
        session_id,
        session_start_timestamp,
        -- marketing attribution
        coalesce(utm_source, referrer_source, channel_source, '(direct)') as source,
        coalesce(utm_medium, referrer_medium, channel_medium, '(none)') as medium,
        utm_campaign,
        utm_term,
        utm_content,
        -- referrer data
        page_referrer,
        page_referrer_host,
        -- Get first session for each user
        row_number() over (
            partition by blended_user_id 
            order by session_start_timestamp asc
        ) as session_number
    from {{ ref('mart_sessions') }}
),

first_sessions as (
    select *
    from first_touch_sessions
    where session_number = 1
),

-- Get user mapping to ensure proper joins
user_mapping as (
    select distinct
        anonymous_id,
        user_id as email
    from {{ ref('stg_users_map') }}
    where user_id is not null
),

-- Combine leads and sales into a single conversion stream
conversions as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        email,
        sign_up_date as conversion_date,
        0 as revenue,
        concat('lead_', email) as conversion_id
    from {{ ref('mart_leads') }}
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        email,
        first_payment_date as conversion_date,
        lifetime_value as revenue,
        concat('sale_', email) as conversion_id
    from {{ ref('mart_sales') }}
),

final as (
    select
        c.conversion_id,
        c.conversion_type,
        c.email,
        c.conversion_date,
        c.revenue,
        -- First touch attribution
        fs.source as first_touch_source,
        fs.medium as first_touch_medium,
        fs.utm_campaign as first_touch_campaign,
        fs.utm_term as first_touch_term,
        fs.utm_content as first_touch_content,
        fs.page_referrer as first_touch_referrer,
        fs.page_referrer_host as first_touch_referrer_host,
        fs.session_start_timestamp as first_touch_timestamp,
        fs.session_id as first_touch_session_id,
        -- Time to convert
        timestamp_diff(
            c.conversion_date,
            fs.session_start_timestamp,
            day
        ) as days_to_convert
    from conversions c
    left join user_mapping um on c.email = um.email
    left join first_sessions fs 
        on um.anonymous_id = fs.anonymous_id
        or c.email = fs.blended_user_id
)

select * from final
