WITH fact AS (
    SELECT *
    FROM {{ ref('fact_checkout_success') }}
),

dim_product AS (
    SELECT *
    FROM {{ ref('dim_product') }}
),

dim_location AS (
    SELECT *
    FROM {{ ref('dim_location') }}
)

SELECT
    f.order_id,
    f.event_time,
    f.local_event_time,
    f.user_id,
    f.device_id,
    f.store_id,
    f.product_id,
    p.product_name,
    f.price,
    f.amount,
    f.revenue,
    f.ip,
    l.city_name,
    l.region_name,
    l.country_name
FROM fact f
LEFT JOIN dim_product p ON f.product_id = p.product_id
LEFT JOIN dim_location l ON f.ip = l.ip
