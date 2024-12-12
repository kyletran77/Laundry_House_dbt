{{ config(
    materialized = 'incremental',
    unique_key = 'email',
    sort = 'first_payment_date',
    partition_by = {
        'field': 'first_payment_date',
        'data_type': 'timestamp'
    },
    cluster_by = 'email'
) }}

with customer_sales as (
    select
        email,
        phone_number,
        total_payment,
        registration_date,
        row_number() over (
            partition by email 
            order by registration_date asc
        ) as row_num
    from {{ ref('stg_customer_sales') }}
    where email is not null
),

aggregated_sales as (
    select
        email,
        min(phone_number) as phone_number,
        sum(total_payment) as lifetime_value,
        min(registration_date) as first_payment_date
    from customer_sales
    group by email
)

select * from aggregated_sales