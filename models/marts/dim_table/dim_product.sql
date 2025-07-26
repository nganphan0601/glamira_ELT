WITH product_dimension AS (
    SELECT 
        product_id,
        product_name
    FROM {{ source('raw_data', 'product_name') }} 
)

SELECT * 
FROM product_dimension