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
        emails,  -- This is now an array field
        first_registration_date
    from {{ ref('int_standardized_users') }}
),

leads_with_emails as (
    select
        su.user_id,
        lower(trim(ue.email)) as email,
        min(ue.first_seen_at) as sign_up_date
    from user_events ue
    inner join standardized_users su
        on lower(trim(ue.email)) in unnest(su.emails)  -- Changed to match the emails array field
    where 
        ue.email is not null
        and ue.email != ''
        and ue.email like '%@%'  -- basic email validation
    group by su.user_id, ue.email
)

select * from leads_with_emails