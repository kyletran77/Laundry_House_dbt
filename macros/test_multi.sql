{{ config(
    materialized = 'view'
) }}

with attribution_totals as (
    select 
        sum(weighted_revenue) as attributed_revenue,
        sum(weighted_lead) as attributed_leads,
        sum(weighted_sale) as attributed_sales
    from {{ ref('mart_conversions_multi_touch') }}
    where conversion_date > timestamp('2024-12-04')
),

source_totals as (
    select 
        sum(lifetime_value) as total_revenue,
        count(*) as total_sales
    from {{ ref('mart_sales') }}
    where cast(first_payment_date as timestamp) > timestamp('2024-12-04')
),

lead_totals as (
    select count(*) as total_leads
    from {{ ref('mart_leads') }}
    where cast(sign_up_date as timestamp) > timestamp('2024-12-04')
)

select
    'Revenue' as metric,
    attributed_revenue as attributed_total,
    total_revenue as source_total
from attribution_totals, source_totals

union all

select
    'Leads' as metric,
    attributed_leads as attributed_total,
    total_leads as source_total
from attribution_totals, lead_totals

union all

select
    'Sales' as metric,
    attributed_sales as attributed_total,
    total_sales as source_total
from attribution_totals, source_total