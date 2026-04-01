SELECT
    pr.product_id, 
    pr.product_name,
    SUM(od.unit_price*od.quantity*(1-od.discount)) AS revenue,
    RANK() OVER (
        ORDER BY
            SUM(od.unit_price * od.quantity *(1 - od.discount)) DESC
    ) AS ranking
FROM
    {{ ref('stg_products') }} pr
JOIN {{ ref('stg_order_details') }} od 
    ON od.product_id = pr.product_id
GROUP BY
    pr.product_id,
    pr.product_name