SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp
FROM {{ source('retail_source', 'orders') }}
