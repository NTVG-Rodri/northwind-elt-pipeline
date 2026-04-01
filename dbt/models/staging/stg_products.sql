SELECT
    product_id,
    TRIM(product_name) AS product_name,
    supplier_id,
    category_id,
    TRIM(quantity_per_unit) AS quantity_per_unit,
    unit_price :: DECIMAL(10,2) AS unit_price,
    units_in_stock :: INT AS units_in_stock,
    units_on_order :: INT AS units_on_order,
    reorder_level :: INT AS reorder_level,
    discontinued :: INT AS discontinued
FROM 
    {{ source('public', 'products')}}
