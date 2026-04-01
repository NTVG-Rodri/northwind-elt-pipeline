WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', o.order_date) AS month,
        SUM(od.unit_price * od.quantity * (1 - od.discount)) :: numeric AS revenue
    FROM
        {{ ref('stg_orders') }} o
        JOIN {{ ref('stg_order_details') }} od ON o.order_id = od.order_id
    GROUP BY
        1
)
SELECT
    month,
    ROUND(revenue, 2) AS revenue,
    ROUND(SUM(revenue) OVER (
            ORDER BY
                month ASC
        ),
        2
    ) AS cumulative_revenue
FROM
    monthly_revenue
ORDER BY
    month