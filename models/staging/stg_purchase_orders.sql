{{ config(
    materialized = 'ephemeral'
) }}

SELECT * FROM staging.purchase_orders