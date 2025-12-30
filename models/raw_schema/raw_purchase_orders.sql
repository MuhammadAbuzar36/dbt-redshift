{{ config(
    materialized='incremental',
    unique_key='purchase_order_id',
    incremental_strategy = 'delete+insert',
) }}

with po_cte as (
    select
        purchase_order_id,
        hub_code,
        status,
        total_amount,
        currency,
        item_number,
        remark,
        created_by,
        created_at,
        order_details,
        seller,
        supplier,
        purchase_order_items,
        grn_details,
        expected_receiving_date,
        -- Deduplication logic in case of duplicate IDs in source
        ROW_NUMBER() OVER (PARTITION BY purchase_order_id ORDER BY created_at DESC) AS row_num
    from {{ ref('stg_purchase_orders') }}
    
    {% if is_incremental() %}
        -- Only pulls records created after the latest record currently in your dbt table
        WHERE created_at >= (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

select 
    purchase_order_id,
    hub_code,
    status,
    total_amount,
    currency,
    item_number,
    remark,
    created_by,
    created_at,
    order_details,
    seller,
    supplier,
    purchase_order_items,
    grn_details,
    expected_receiving_date
from po_cte
where row_num = 1