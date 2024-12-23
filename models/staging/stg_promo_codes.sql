{% set tables = dbt_utils.get_relations_by_pattern(
    schema_pattern='promo_folder_2',
    table_pattern='%promo_codes%'
) %}

{% if tables|length > 0 %}
    {% for table in tables %}
        select * from {{ table }}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% else %}
    select null as no_tables_found limit 0
{% endif %}