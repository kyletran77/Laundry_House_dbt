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

standardized_accounts as (
    select
        cast(user_id as string) as user_id,
        first_registration_date,
        last_usage_date,
        total_payments,
        total_cycle_usage,
        combined_balance,
        number_of_merged_accounts
    from {{ ref('int_standardized_accounts') }}
),

conversions_raw as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        su.user_id,
        l.email,
        cast(l.sign_up_date as timestamp) as conversion_date,
        cast(0 as numeric) as revenue,
        concat('lead_', su.user_id) as conversion_id,
        sa.first_registration_date,
        sa.last_usage_date,
        sa.total_payments,
        sa.total_cycle_usage,
        sa.combined_balance,
        sa.number_of_merged_accounts
    from {{ ref('mart_leads') }} l
    inner join standardized_users su
        on l.email in unnest(su.emails)
    left join standardized_accounts sa
        on su.user_id = sa.user_id
    where cast(sign_up_date as timestamp) > timestamp('2024-12-04')
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        su.user_id,
        s.email,
        cast(s.first_payment_date as timestamp) as conversion_date,
        s.lifetime_value as revenue,
        concat('sale_', su.user_id) as conversion_id,
        sa.first_registration_date,
        sa.last_usage_date,
        sa.total_payments,
        sa.total_cycle_usage,
        sa.combined_balance,
        sa.number_of_merged_accounts
    from {{ ref('mart_sales') }} s
    inner join standardized_users su
        on s.email in unnest(su.emails)
    left join standardized_accounts sa
        on su.user_id = sa.user_id
    where cast(first_payment_date as timestamp) > timestamp('2024-12-04')
),

conversions as (
    select distinct * from conversions_raw  -- Ensure uniqueness at this level
),

user_sessions as (
    select distinct
        su.user_id,
        s.session_id,
        s.session_start_timestamp,
        s.utm_source,
        s.referrer_source,
        s.channel_source,
        s.utm_medium,
        s.referrer_medium,
        s.channel_medium,
        s.utm_campaign,
        s.utm_term,
        s.utm_content,
        s.page_referrer,
        s.page_referrer_host
    from standardized_users su
    cross join unnest(su.emails) as email
    left join {{ ref('mart_sessions') }} s
        on s.blended_user_id = email
    where s.session_start_timestamp > timestamp('2024-12-04')
),

user_first_sessions as (
    select
        user_id,
        first_value(session_id) over (
            partition by user_id 
            order by session_start_timestamp asc
        ) as first_session_id,
        min(session_start_timestamp) over (
            partition by user_id
        ) as first_session_timestamp
    from user_sessions
),

final as (
    select distinct  -- Ensure final uniqueness
        c.conversion_id,
        c.conversion_type,
        c.user_id,
        c.email,
        c.conversion_date,
        c.revenue,
        -- Account data
        c.first_registration_date,
        c.last_usage_date,
        c.total_payments,
        c.total_cycle_usage,
        c.combined_balance,
        c.number_of_merged_accounts,
        -- First touch attribution
        fs.utm_source as first_touch_source,
        fs.utm_medium as first_touch_medium,
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
        on c.user_id = ufs.user_id
    left join user_sessions fs 
        on ufs.first_session_id = fs.session_id
)

select * from final