select count(*) 
from {{ ref('mart_sales') }}
where cast(first_payment_date as timestamp) > timestamp('2024-12-04')