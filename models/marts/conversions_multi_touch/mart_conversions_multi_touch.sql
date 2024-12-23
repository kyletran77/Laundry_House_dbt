{{ config(
    materialized = 'incremental',
    unique_key = 'attribution_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['user_id']
) }}

with standardized_users as (
    select
        user_id,
        emails[offset(0)] as primary_email,
        emails as all_emails
    from {{ ref('int_standardized_users') }}
),

conversion_sessions as (
    select
        c.conversion_id,
        c.user_id,
        c.email,
        c.conversion_date,
        c.revenue,
        c.conversion_type,
        s.session_id,
        s.session_start_timestamp,
        coalesce(
            cast(s.utm_source as string),
            cast(s.referrer_source as string),
            cast(s.channel_source as string),
            '(direct)'
        ) as source,
        coalesce(
            cast(s.utm_medium as string),
            cast(s.referrer_medium as string),
            cast(s.channel_medium as string),
            '(none)'
        ) as medium,
        cast(s.utm_campaign as string) as utm_campaign,
        row_number() over (
            partition by c.conversion_id 
            order by s.session_start_timestamp asc
        ) as session_number,
        count(*) over (
            partition by c.conversion_id
        ) as total_sessions
    from {{ ref('mart_conversions_first_touch') }} c
    left join {{ ref('mart_sessions') }} s
        on c.user_id = s.blended_user_id
        and s.session_start_timestamp <= c.conversion_date
        where c.conversion_date > timestamp('2024-12-04')
),

attribution_weights as (
    select 
        *,
        case
            when total_sessions = 1 then 1.0  -- Single session gets 100%
            when total_sessions = 2 then 0.5  -- Two sessions split 50/50
            when total_sessions > 2 then
                case
                    when session_number = 1 then 0.4  -- First touch
                    when session_number = total_sessions then 0.4  -- Last touch
                    else 0.2 / (total_sessions - 2)  -- Middle touches split remaining 20%
                end
        end as weight
    from conversion_sessions
)

select
    concat(conversion_id, '_', session_id) as attribution_id,
    email,
    conversion_date,  -- Added this field
    conversion_type,
    source,
    medium,
    utm_campaign,
    case when conversion_type = 'lead' then weight else 0 end as weighted_lead,
    case when conversion_type = 'sale' then weight else 0 end as weighted_sale,
    revenue * weight as weighted_revenue
from attribution_weights