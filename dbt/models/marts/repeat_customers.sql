SELECT
    c.customer_id,
    c.company_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    MIN(o.order_date) AS first_purchase,
    MAX(o.order_date) AS last_purchase
FROM 
    {{ ref('stg_customers') }} c
JOIN {{ ref('stg_orders') }} o
    ON o.customer_id = c.customer_id
JOIN {{ ref('stg_order_details') }} od
    ON od.order_id = o.order_id
GROUP BY
    1, 2
HAVING
    COUNT(DISTINCT o.order_id) > 1
ORDER BY
    total_orders DESC