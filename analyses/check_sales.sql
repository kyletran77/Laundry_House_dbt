select sum(weighted_sale) 
from {{ ref('mart_conversions_multi_touch') }}