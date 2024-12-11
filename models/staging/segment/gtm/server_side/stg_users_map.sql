{% set page_table = source("gtm_server_side", "pages") %}
{% set tracks_table = source("gtm_server_side", "tracks") %}
{% set identifies_table = source("gtm_server_side", "identifies") %}


with
    anonymous_id_user_id_pairs as (
        select distinct anonymous_id, user_id, sent_at
        from
            (
                select anonymous_id, user_id, sent_at
                from {{ page_table }}
                union all
                select anonymous_id, user_id, sent_at
                from {{ tracks_table }}
                union all
                select anonymous_id, user_id, sent_at
                from {{ identifies_table }}
            ) as combined
            
    )

select distinct
    anonymous_id,
    last_value(user_id) over (
        partition by anonymous_id
        order by anonymous_id_user_id_pairs.sent_at asc
        rows between unbounded preceding and unbounded following
    ) as user_id
from anonymous_id_user_id_pairs
where user_id is not null
