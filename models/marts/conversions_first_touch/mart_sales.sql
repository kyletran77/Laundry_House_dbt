{{ config(
    materialized = 'incremental',
    unique_key = 'email',  -- Using email as the unique identifier
    sort = 'payment_date',
    partition_by = {'field': 'payment_date', 'data_type': 'timestamp'},
    cluster_by = 'email'
) }}

with customer_sales as (
    select
        email,
        phone_number,
        total_payment,
        payment_date,  -- this is the registration_date from staging
        row_number() over (
            partition by email 
            order by payment_date asc
        ) as row_num
    from {{ ref('stg_customer_sales') }}
    where email is not null  -- ensure we have valid emails
),

aggregated_sales as (
    select
        email,
        min(phone_number) as phone_number,  -- take the first phone number
        sum(total_payment) as lifetime_value,  -- total payments across all records
        min(payment_date) as first_payment_date  -- first transaction date
    from customer_sales
    group by email
)

select * from aggregated_sales
