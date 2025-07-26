with raw_summary as (
    SELECT
        _id,
        order_id,
        TIMESTAMP_SECONDS(time_stamp) AS event_time,
        PARSE_TIMESTAMP('%F %H:%M:%S', local_time) AS local_event_time,
        ip,
        NULLIF(user_id_db, '') AS user_id,
        device_id,
        store_id,
        JSON_EXTRACT_ARRAY(cart_products) AS cart_items
    FROM {{ source('raw_data', 'summary_users') }}
    WHERE collection = 'checkout_success'
)

SELECT * FROM raw_summary
