WITH customer_revenue AS (
    SELECT
        c.customer_id,
        SUM(od.unit_price*od.quantity*(1 - od.discount))::numeric AS total_spent
    FROM
        {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o   
        ON c.customer_id = o.customer_id
    JOIN {{ ref('stg_order_details') }} od 
        ON o.order_id = od.order_id
    GROUP BY
        1
),
ranked_customer AS (
    SELECT
        *,
        NTILE(3) OVER (
            ORDER BY total_spent DESC
        ) AS third
    FROM 
        customer_revenue
)

SELECT
    *,
    CASE
        WHEN third = 1 THEN 'HIGH'
        WHEN third = 2 THEN 'MEDIUM'
        WHEN third = 3 THEN 'LOW'
    END AS segment
FROM 
    ranked_customer