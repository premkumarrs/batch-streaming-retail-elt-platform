SELECT
    customer_id,
    customer_city,
    customer_state
FROM {{ source('retail_source', 'customers') }}
