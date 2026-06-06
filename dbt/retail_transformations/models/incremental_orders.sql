{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    customer_id,
    order_status
FROM streaming_orders

{% if is_incremental() %}

WHERE order_id NOT IN (SELECT order_id FROM {{ this }})

{% endif %}