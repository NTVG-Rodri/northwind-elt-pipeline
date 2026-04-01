SELECT
    employee_id,
    TRIM(last_name) AS last_name,
    TRIM(first_name) AS first_name,
    TRIM(title) AS title,
    title_of_courtesy :: VARCHAR(4) AS title_of_courtesy,
    birth_date :: DATE AS birth_date,
    hire_date :: DATE AS hire_date,
    TRIM(address) AS address,
    TRIM(city) AS city,
    TRIM(region) AS region,
    TRIM(postal_code) AS postal_code,
    TRIM(country) AS country,
    TRIM(home_phone) AS home_phone,
    TRIM(extension) AS extension,
    TRIM(notes) AS notes,
    reports_to :: INT AS reports_to
FROM
    {{ source('public', 'employees') }}

