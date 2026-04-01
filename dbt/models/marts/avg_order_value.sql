WITH order_values AS (
    SELECT
        o.order_id,
        o.customer_id,
        SUM(od.unit_price*od.quantity*(1-od.discount))::numeric AS order_amount
    FROM
        {{ ref('stg_orders') }} o  
    JOIN {{ ref('stg_order_details') }} od 
        ON od.order_id = o.order_id
    GROUP BY
        1, 2
),
customer_history AS (
    SELECT
        c.customer_id,
        c.company_name,
        SUM(ov.order_amount)::numeric AS revenue,
        COUNT(DISTINCT ov.order_id) AS total_orders,
        MIN(ov.order_amount) AS min_order,
        MAX(ov.order_amount) AS max_order,
        AVG(ov.order_amount) AS avg_order
    FROM
        {{ ref('stg_customers') }} c
    JOIN order_values ov
        ON ov.customer_id = c.customer_id
    GROUP BY
        1, 2
)

SELECT
    customer_id,
    company_name,
    ROUND(revenue, 2) AS total_spent,
    ROUND(min_order, 2) AS minimum_ticket,
    ROUND(max_order, 2) AS maximum_ticket,
    ROUND(avg_order, 2) AS avg_ticket
FROM 
    customer_history
ORDER BY
    avg_ticket DESC