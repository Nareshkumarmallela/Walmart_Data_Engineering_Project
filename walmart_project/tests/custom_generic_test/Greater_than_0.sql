SELECT *
FROM {{ ref('products_t') }}
WHERE price < 0