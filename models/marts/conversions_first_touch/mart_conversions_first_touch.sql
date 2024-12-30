{{ config(
    materialized = 'incremental',
    unique_key = 'conversion_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['user_id', 'conversion_type']
) }}

with standardized_users as (
    select
        cast(user_id as string) as user_id,
        emails
    from {{ ref('int_standardized_users') }}
),

conversions as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        su.user_id,
        l.email,
        cast(l.sign_up_date as timestamp) as conversion_date,
        cast(0 as numeric) as revenue,
        concat('lead_', su.user_id) as conversion_id
    from {{ ref('mart_leads') }} l
    inner join standardized_users su
        on l.email in unnest(su.emails)
    where cast(sign_up_date as timestamp) > timestamp('2024-12-04')
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        su.user_id,
        s.email,
        cast(s.first_payment_date as timestamp) as conversion_date,
        s.lifetime_value as revenue,
        concat('sale_', su.user_id) as conversion_id
    from {{ ref('mart_sales') }} s
    inner join standardized_users su
        on s.email in unnest(su.emails)
    where cast(first_payment_date as timestamp) > timestamp('2024-12-04')
),

first_touch_sessions as (
    select
        cast(blended_user_id as string) as blended_user_id,
        anonymous_id,
        session_id,
        session_start_timestamp,
        coalesce(
            cast(utm_source as string),
            cast(referrer_source as string),
            cast(channel_source as string),
            '(direct)'
        ) as source,
        coalesce(
            cast(utm_medium as string),
            cast(referrer_medium as string),
            cast(channel_medium as string),
            '(none)'
        ) as medium,
        utm_campaign,
        utm_term,
        utm_content,
        page_referrer,
        page_referrer_host
    from {{ ref('mart_sessions') }}
    where session_start_timestamp > timestamp('2024-12-04')
),

user_first_sessions as (
    select
        blended_user_id,
        first_value(session_id) over (
            partition by blended_user_id 
            order by session_start_timestamp asc
        ) as first_session_id,
        min(session_start_timestamp) over (
            partition by blended_user_id
        ) as first_session_timestamp
    from first_touch_sessions
),

final as (
    select distinct
        c.conversion_id,
        c.conversion_type,
        c.user_id,
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
        ufs.first_session_timestamp as first_touch_timestamp,
        ufs.first_session_id as first_touch_session_id,
        -- Time to convert
        timestamp_diff(
            c.conversion_date,
            ufs.first_session_timestamp,
            day
        ) as days_to_convert
    from conversions c
    left join user_first_sessions ufs
        on c.email = ufs.blended_user_id  -- Try matching on email first
    left join first_touch_sessions fs 
        on ufs.first_session_id = fs.session_id
)

select * from final