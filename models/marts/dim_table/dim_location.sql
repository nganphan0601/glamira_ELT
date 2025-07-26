WITH ip2location AS (
    SELECT 
        IP AS ip,
        Country AS country_name,
        Region AS region_name,
        City AS city_name
    FROM {{ source('raw_data', 'ip2location') }} 
)

SELECT * 
FROM ip2location