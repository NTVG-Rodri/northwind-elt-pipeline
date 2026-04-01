WITH customer_history AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.country,
        SUM(od.unit_price*od.quantity*(1-od.discount))::numeric AS revenue,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM
        {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o 
        ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_order_details') }} od
        ON od.order_id = o.order_id
    GROUP BY
        1, 2, 3
)

SELECT
    customer_id,
    company_name,
    country,
    ROUND(revenue, 2) AS revenue,
    total_orders,
    RANK() OVER (
        ORDER BY revenue DESC
    ) AS ranking
FROM 
    customer_history
ORDER BY
    ranking ASC