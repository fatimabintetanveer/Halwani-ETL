{{ config(materialized='table') }}

WITH lulu_key AS (
    SELECT
        'Lulu' || SKU || CASE WHEN SizeDesc = '' OR SizeDesc IS NULL THEN '' ELSE '-' || SizeDesc END AS Key,
        s.*
    FROM {{ source('silver', 'silver_lulu') }} s
),

sku_mapping AS ( {{ sku_mapping() }} )

SELECT s.Key, s.SKU, s."Description" AS product_desc, s.Month, COUNT(*) AS row_count,
       SUM(TRY_CAST(s."Sales Amount" AS DOUBLE)) AS unmapped_sales
FROM lulu_key s
LEFT JOIN sku_mapping m ON UPPER(s.Key) = UPPER(m.Key)
WHERE m.Key IS NULL
GROUP BY s.Key, s.SKU, product_desc, s.Month
ORDER BY s.Month, unmapped_sales DESC