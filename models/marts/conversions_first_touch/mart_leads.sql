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
        emails[offset(0)] as primary_email,
        emails as all_emails
    from {{ ref('int_standardized_users') }}
),

leads_with_emails as (
    select
        su.user_id,
        su.primary_email as email,
        min(ue.first_seen_at) as sign_up_date
    from user_events ue
    inner join standardized_users su
        on lower(trim(ue.email)) = su.primary_email
        or lower(trim(ue.email)) in unnest(su.all_emails)
    group by su.user_id, su.primary_email
)

select * from leads_with_emails