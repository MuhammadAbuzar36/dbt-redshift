{{
    config(
        materialized="incremental",
        unique_key="item_id",
        incremental_strategy="delete+insert",
    )
}}

with
    po_items_cte as (
        select
            -- New Unique Key
            item_id,
            purchase_order_id,

            -- Basic Info
            hub_code,
            status,
            total_amount,
            currency,
            created_by,
            created_at,

            -- Flattened Seller & Supplier
            seller_name,
            seller_phone,
            seller_email,
            supplier_name,
            supplier_phone,
            supplier_email,

            total_po_quantity,
            total_grn_pass_qty,
            total_grn_fail_qty,
            total_put_away_pass_qty,
            total_put_away_fail_qty,

            -- Flattened Item Details
            item_sku_name,
            item_sku_code,
            item_ordered_quantity,
            item_received_quantity,
            item_unit_price,
            item_last_updated,

            -- Deduplication logic (Grain is now item_id)
            row_number() over (
                partition by item_id order by item_last_updated desc
            ) as row_num

        -- Using 'ref' assuming you built the unnested table as a dbt model
        -- If you built it manually in Redshift, use: source('staging',
        -- 'unnested_purchase_orders')
        from {{ ref("stg_purchase_orders") }}

        {% if is_incremental() %}
            -- Only pulls records where the item was updated after the last run
            WHERE item_last_updated >= (SELECT MAX(item_last_updated) FROM {{ this }})
        {% endif %}
    )

select
    item_id,
    purchase_order_id,
    hub_code,
    status,
    total_amount,
    currency,
    created_by,
    created_at,
    seller_name,
    seller_phone,
    seller_email,
    supplier_name,
    supplier_phone,
    supplier_email,
    total_po_quantity,
    total_grn_pass_qty,
    total_grn_fail_qty,
    total_put_away_pass_qty,
    total_put_away_fail_qty,
    item_sku_name,
    item_sku_code,
    item_ordered_quantity,
    item_received_quantity,
    item_unit_price,
    item_last_updated
from po_items_cte
where row_num = 1
