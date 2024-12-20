{{ config(
    materialized = 'table',
    partition_by = {
        'field': 'summary_report_date',
        'data_type': 'date'
    },
    cluster_by = ['_fivetran_synced']
) }}

WITH latest_syncs AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY summary_report_date 
            ORDER BY _fivetran_synced DESC
        ) as rn
    FROM {{ source('summary_report_gcf', 'summary_reports') }}
),

parsed_data AS (
    SELECT 
        summary_report_date,
        extracted_at,
        _fivetran_synced,
        JSON_EXTRACT_SCALAR(`all`, '$.num_machines') as total_machines,
        JSON_EXTRACT_SCALAR(`all`, '$.machines.dryer_cycle_total') as dryer_cycle_total,
        JSON_EXTRACT_SCALAR(`all`, '$.machines.washer_cycle_total') as washer_cycle_total,
        JSON_EXTRACT_SCALAR(`all`, '$.machines.vending_cycle_total') as vending_cycle_total,
        JSON_EXTRACT_SCALAR(`all`, '$.machines.mobile_cycle_total') as mobile_cycle_total,
        JSON_EXTRACT_SCALAR(_4097, '$.location_key') as location_key,
        JSON_EXTRACT_SCALAR(_4097, '$.location_ID') as location_id,
        JSON_EXTRACT_SCALAR(_4097, '$.location_full_address') as location_full_address,
        JSON_EXTRACT_SCALAR(_4097, '$.num_machines') as location_machines
    FROM latest_syncs
    WHERE rn = 1  -- Get only the most recent sync for each date
)

SELECT 
    summary_report_date,
    extracted_at,
    _fivetran_synced,
    -- CAST(total_machines AS INT64) as total_machines,
    CAST(dryer_cycle_total AS FLOAT64) as dryer_cycle_total,
    CAST(washer_cycle_total AS FLOAT64) as washer_cycle_total,
    CAST(vending_cycle_total AS FLOAT64) as vending_cycle_total,
    CAST(mobile_cycle_total AS FLOAT64) as mobile_cycle_total,
    -- CAST(location_key AS INT64) as location_key,
    -- location_id,
    -- location_full_address,
    -- CAST(location_machines AS INT64) as location_machines
FROM parsed_data