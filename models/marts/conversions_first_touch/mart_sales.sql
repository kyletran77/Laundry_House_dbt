{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    sort = 'first_payment_date',
    partition_by = {
        'field': 'first_payment_date',
        'data_type': 'timestamp'
    },
    cluster_by = 'user_id'
) }}

with standardized_accounts as (
    select
        user_id,
        first_registration_date as first_payment_date,
        total_payments as lifetime_value
    from {{ ref('int_standardized_accounts') }}
),

standardized_users as (
    select
        user_id,
        emails[offset(0)] as email
    from {{ ref('int_standardized_users') }}
)

select
    sa.user_id,
    su.email,
    sa.lifetime_value,
    sa.first_payment_date
from standardized_accounts sa
join standardized_users su using (user_id)