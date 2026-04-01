SELECT
    order_id,
    TRIM(customer_id) AS customer_id,
    employee_id,
    order_date :: DATE AS order_date,
    required_date :: DATE AS required_date,
    shipped_date :: DATE AS shipped_date,
    ship_via,
    freight :: NUMERIC(10, 2) AS freight,
    TRIM(ship_name) AS ship_name,
    TRIM(ship_address) AS ship_address,
    TRIM(ship_city) AS ship_city,
    TRIM(ship_region) AS ship_region,
    ship_postal_code :: VARCHAR AS ship_postal_code,
    TRIM(ship_country) AS ship_country
FROM
    {{ source('public', 'orders') }}
