{{ config(
    materialized = 'incremental',
    unique_key = 'conversion_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['user_id', 'conversion_type']
) }}

with standardized_users as (
    select
        user_id,
        emails[offset(0)] as primary_email,
        emails as all_emails
    from {{ ref('int_standardized_users') }}
),

conversions as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        l.user_id,
        l.email,
        cast(l.sign_up_date as timestamp) as conversion_date,
        cast(0 as numeric) as revenue,
        concat('lead_', l.user_id) as conversion_id
    from {{ ref('mart_leads') }} l
    where cast(sign_up_date as timestamp) > timestamp('2024-12-04')
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        s.user_id,
        s.email,
        cast(s.first_payment_date as timestamp) as conversion_date,
        s.lifetime_value as revenue,
        concat('sale_', s.user_id) as conversion_id
    from {{ ref('mart_sales') }} s
    where cast(first_payment_date as timestamp) > timestamp('2024-12-04')
)

first_touch_sessions as (
    select
        blended_user_id,
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
    where session_start_timestamp > timestamp('2024-12-04')  -- After Dec 4th
),

final as (
    select distinct
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
    left join first_touch_sessions fs 
        on c.user_id = fs.blended_user_id
        and fs.session_start_timestamp <= c.conversion_date  -- Only include sessions before conversion
    qualify row_number() over (
        partition by c.conversion_id 
        order by fs.session_start_timestamp asc
    ) = 1  -- Get the first session for each conversion
)

select * from final