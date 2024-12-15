select sum(weighted_lead) 
from {{ ref('mart_conversions_multi_touch') }}
