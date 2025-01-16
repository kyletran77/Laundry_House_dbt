{{ config(
    materialized = 'incremental',
    unique_key = 'attribution_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['user_id']
) }}

with standardized_users as (
    select
        cast(user_id as string) as user_id,
        emails,
        phone_numbers
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

-- Get all sessions for each user by matching any of their identifiers
user_sessions as (
    select distinct
        su.user_id,
        sa.first_registration_date,
        sa.last_usage_date,
        sa.total_payments,
        sa.total_cycle_usage,
        sa.combined_balance,
        sa.number_of_merged_accounts,
        s.session_id,
        s.session_start_timestamp,
        s.utm_source,
        s.referrer_source,
        s.channel_source,
        s.utm_medium,
        s.referrer_medium,
        s.channel_medium,
        s.utm_campaign
    from standardized_users su
    left join standardized_accounts sa 
        on su.user_id = sa.user_id
    cross join unnest(su.emails) as email
    left join {{ ref('mart_sessions') }} s
        on s.blended_user_id = email
),

conversion_sessions as (
    select
        c.conversion_id,
        c.user_id,
        c.email,
        c.conversion_date,
        c.revenue,
        c.conversion_type,
        c.first_registration_date,
        c.last_usage_date,
        c.total_payments,
        c.total_cycle_usage,
        c.combined_balance,
        c.number_of_merged_accounts,
        us.session_id,
        us.session_start_timestamp,
        coalesce(
            cast(us.utm_source as string),
            cast(us.referrer_source as string),
            cast(us.channel_source as string),
            '(direct)'
        ) as source,
        coalesce(
            cast(us.utm_medium as string),
            cast(us.referrer_medium as string),
            cast(us.channel_medium as string),
            '(none)'
        ) as medium,
        cast(us.utm_campaign as string) as utm_campaign,
        row_number() over (
            partition by c.conversion_id 
            order by us.session_start_timestamp asc
        ) as session_number,
        count(*) over (
            partition by c.conversion_id
        ) as total_sessions
    from {{ ref('mart_conversions_first_touch') }} c
    left join user_sessions us
        on us.user_id = c.user_id
        and us.session_start_timestamp <= c.conversion_date
    where c.conversion_date > timestamp('2024-12-04')
),

attribution_weights as (
    select 
        *,
        case
            when total_sessions = 1 then 1.0
            when total_sessions = 2 then 0.5
            when total_sessions > 2 then
                case
                    when session_number = 1 then 0.4
                    when session_number = total_sessions then 0.4
                    else 0.2 / (total_sessions - 2)
                end
        end as weight
    from conversion_sessions
)

select
    concat(conversion_id, '_', coalesce(session_id, 'no_session')) as attribution_id,
    user_id,
    email,
    conversion_date,
    conversion_type,
    first_registration_date,
    last_usage_date,
    total_payments,
    total_cycle_usage,
    combined_balance,
    number_of_merged_accounts,
    coalesce(source, '(no source)') as source,
    coalesce(medium, '(no medium)') as medium,
    coalesce(utm_campaign, '(no campaign)') as utm_campaign,
    case when conversion_type = 'lead' then weight else 0 end as weighted_lead,
    case when conversion_type = 'sale' then weight else 0 end as weighted_sale,
    revenue * weight as weighted_revenue
from attribution_weights