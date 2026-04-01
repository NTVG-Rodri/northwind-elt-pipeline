SELECT
    cat.category_id,
    cat.category_name,
    SUM(od.unit_price*quantity*(1-od.discount)) AS revenue
FROM
    {{ ref('stg_categories') }} cat
JOIN {{ ref('stg_products') }} pr 
    ON pr.category_id = cat.category_id
JOIN {{ ref('stg_order_details') }} od 
    ON pr.product_id = od.product_id
GROUP BY 
    cat.category_id,
    cat.category_name