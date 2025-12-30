{{ config(
    materialized = 'ephemeral'
) }}

SELECT * FROM staging.stg_purchase_orders