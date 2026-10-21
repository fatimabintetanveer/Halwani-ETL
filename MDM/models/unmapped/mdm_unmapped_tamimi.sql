{{ config(materialized='table') }}

WITH tamimi_key AS (
    SELECT
        'Tamimi' || SKU || "Sales Unit" || CASE WHEN SizeDesc = '' OR SizeDesc IS NULL THEN '' ELSE '-' || SizeDesc END AS Key,
        s.*
    FROM {{ source('silver', 'silver_tamimi') }} s
),

sku_mapping AS ( {{ sku_mapping() }} )

SELECT
    s.Key, s.SKU, s."SKU Description" AS product_desc,
    s.Month, COUNT(*) AS row_count,
    SUM(TRY_CAST(s."Sales Amount" AS DOUBLE)) AS unmapped_sales
FROM tamimi_key s
LEFT JOIN sku_mapping m ON UPPER(s.Key) = UPPER(m.Key)
WHERE m.Key IS NULL
GROUP BY s.Key, s.SKU, product_desc, s.Month
ORDER BY s.Month, unmapped_sales DESC