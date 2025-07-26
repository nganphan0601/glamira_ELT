-- Flatten the cart_products array
WITH products_flattened AS (
    SELECT
        order_id,
        event_time,
        local_event_time,
        ip,
        user_id,
        device_id,
        store_id,
        JSON_EXTRACT_SCALAR(cp, '$.product_id') AS product_id,
        SAFE_CAST(REPLACE(JSON_EXTRACT_SCALAR(cp, '$.price'), ',', '') AS FLOAT64) AS price,
        SAFE_CAST(JSON_EXTRACT_SCALAR(cp, '$.amount') AS INT64) AS amount,
        JSON_EXTRACT_SCALAR(cp, '$.currency') AS currency,
        
        SAFE_CAST(REPLACE(JSON_EXTRACT_SCALAR(cp, '$.price'), ',', '') AS FLOAT64) *
        SAFE_CAST(JSON_EXTRACT_SCALAR(cp, '$.amount') AS INT64) AS revenue
    FROM {{ ref('stg_checkout_success') }},
         UNNEST(cart_items) AS cp
)

SELECT *
FROM products_flattened