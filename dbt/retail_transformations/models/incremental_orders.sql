{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    product,
    category,
    price,
    quantity,
    order_date
FROM streaming_orders

{% if is_incremental() %}

WHERE order_date >
(
    SELECT MAX(order_date)
    FROM {{ this }}
)

{% endif %}