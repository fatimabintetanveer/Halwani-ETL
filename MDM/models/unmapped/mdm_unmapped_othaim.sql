{{ config(materialized='table') }}

WITH othaim_key AS (
    SELECT 'Othaim' || SKU || CASE WHEN SizeDesc = '' OR SizeDesc IS NULL THEN '' ELSE '-' || SizeDesc END AS Key, s.*
    FROM {{ source('silver', 'silver_othaim') }} s
),

sku_mapping AS ( {{ sku_mapping() }} )

SELECT s.Key, s.SKU, s."SKU Description" AS product_desc, s.Month, COUNT(*) AS row_count,
       SUM(s."Sales Amount") AS unmapped_sales
FROM othaim_key s
LEFT JOIN sku_mapping m ON s.Key = m.Key
WHERE m.Key IS NULL
GROUP BY s.Key, s.SKU, product_desc, s.Month
ORDER BY s.Month, unmapped_sales DESC