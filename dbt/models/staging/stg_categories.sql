SELECT
    category_id,
    TRIM(category_name) AS category_name,
    TRIM(description) AS description
FROM
    {{ source('public', 'categories') }}