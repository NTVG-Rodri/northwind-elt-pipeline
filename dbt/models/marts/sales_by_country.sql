SELECT
    ship_country,
    COUNT(order_id) AS total_orders,
    SUM(freight) AS total_freight
FROM
    {{ ref('stg_orders') }}
GROUP BY 
    ship_country