{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    sort = 'sign_up_date',
    partition_by = {'field': 'sign_up_date', 'data_type': 'timestamp'},
    cluster_by = 'user_id'
) }}

with user_events as (
    select
        anonymous_id,
        user_id as email,
        min(loaded_at) as first_seen_at
    from {{ source('gtm_server_side', 'identifies') }}
    where user_id is not null
    group by anonymous_id, user_id
),

standardized_users as (
    select
        user_id,
        emails
    from {{ ref('int_standardized_users') }}
),

-- Get earliest lead for each user
leads_with_emails as (
    select
        su.user_id,
        array_agg(ue.email order by ue.first_seen_at asc limit 1)[offset(0)] as email,
        min(ue.first_seen_at) as sign_up_date
    from user_events ue
    inner join standardized_users su
        on lower(trim(ue.email)) in unnest(su.emails)
    where 
        ue.email is not null
        and ue.email != ''
        and ue.email like '%@%'
    group by su.user_id
)

select * from leads_with_emails