{{ config(materialized='table') }}

SELECT
    s.Key,
    s.SKU,
    s.Retailer,
    s.Month,
    COUNT(*) AS row_count,
    SUM(TRY_CAST(s."Sales Amount" AS DOUBLE)) AS unmapped_sales
FROM {{ source('silver', 'silver_unified') }} s
ANTI JOIN {{ ref('master_sku_mapping') }} m
    ON UPPER(TRIM(s.Key)) = UPPER(TRIM(m.Key))
GROUP BY s.Key, s.SKU, s.Retailer, s.Month
ORDER BY s.Retailer, s.Month, unmapped_sales DESC