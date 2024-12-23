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

with standardized_users as (
    select
        user_id,
        emails[offset(0)] as primary_email,
        phone_numbers[offset(0)] as primary_phone,
        first_registration_date,
        total_payments as lifetime_value
    from {{ ref('int_standardized_users') }}
)

select
    user_id,
    primary_email as email,
    primary_phone as phone_number,
    lifetime_value,
    first_registration_date as first_payment_date
from standardized_users