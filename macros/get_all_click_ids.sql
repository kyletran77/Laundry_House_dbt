{% macro get_all_click_ids(context_page_url) %}

{% set click_ids = [
    "fbclid",
    "gclid",
    "wbraid",
    "gbraid",
    "ttclid",
    "li_fat_id",  
    "epik",    
    "msclkid",
    "twclid",
    "_kx",

] %}


--   "hsa_acc",    
    -- "yclid",
    -- "dclid",
    -- "s_kwcid",
    -- "ef_id",
    -- "dm_i",
    -- "mc_cid",
    -- "mc_eid",
    -- "__s",
    -- "ml_subscriber",
    -- "s_cid",
    -- "igshid",
    -- "si",
    -- "_branch_match_id",
{% for click_id in click_ids %}
    {{ dbt_utils.get_url_parameter(context_page_url, click_id) }} as {{ click_id }}{% if not loop.last %},{% endif %}
{% endfor %}

{% endmacro %}
