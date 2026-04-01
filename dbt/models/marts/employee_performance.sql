WITH employee_history AS (
    SELECT
        e.employee_id,
        e.last_name || ' ' || e.first_name AS full_name,
        SUM(od.unit_price*od.quantity*(1-od.discount))::numeric AS revenue,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM 
        {{ ref('stg_employees') }} e
    JOIN {{ ref('stg_orders') }} o 
        ON o.employee_id = e.employee_id
    JOIN {{ ref('stg_order_details') }} od
        ON od.order_id = o.order_id
    GROUP BY
        1, 2
)

SELECT
    employee_id,
    full_name,
    ROUND(revenue, 2) AS revenue,
    total_orders,
    RANK() OVER (
        ORDER BY
            total_orders DESC
    ) AS ranking
FROM
    employee_history
ORDER BY
    ranking ASC