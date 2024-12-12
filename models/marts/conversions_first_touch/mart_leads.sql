{{ config(
    materialized = 'incremental',
    unique_key = 'email',
    sort = 'sign_up_date',
    partition_by = {'field': 'sign_up_date', 'data_type': 'timestamp'},
    cluster_by = 'email'
) }}

with user_events as (
    select
        anonymous_id,
        user_id as email,
        min(loaded_at) as first_seen_at
    from {{ source('gtm_server_side', 'identifies') }}  -- Changed to use source directly
    where user_id is not null
    group by anonymous_id, user_id
),

leads_with_emails as (
    select
        lower(trim(email)) as email,
        min(first_seen_at) as sign_up_date
    from user_events
    where 
        email is not null
        and email != ''
        and email like '%@%'  -- basic email validation
    group by email
)

select * from leads_with_emails