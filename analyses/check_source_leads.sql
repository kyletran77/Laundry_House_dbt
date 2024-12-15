select count(*) 
from {{ ref('mart_leads') }}
where cast(sign_up_date as timestamp) > timestamp('2024-12-04')