SELECT
    order_id,
    product_id,
    unit_price :: DECIMAL(10,2) AS unit_price,
    quantity :: INT AS quantity,
    discount :: DECIMAL(3,2) AS discount
FROM 
    {{ source('public', 'order_details') }}