{{ config(materialized='table') }}

SELECT DISTINCT
    s."Store Num" AS store_num,
    s."Store Name" AS store_name,
    s.Month,
    s.StoreType AS store_type,
    s.City AS city,
    s.Region AS region,
    COUNT(*) AS row_count
FROM {{ source('silver', 'silver_unified') }} s
ANTI JOIN {{ ref('master_store_mapping') }} sm
    ON sm."Src_Store Num" = TRY_CAST(s."Store Num" AS INTEGER)
    AND REGEXP_REPLACE(UPPER(sm."Src_Store Name"), '[^A-Z0-9]', '', 'g') = REGEXP_REPLACE(UPPER(s."Store Name"), '[^A-Z0-9]', '', 'g')
WHERE s.Retailer = 'Panda'
GROUP BY s."Store Num", s."Store Name", s.Month, s.StoreType, s.City, s.Region
ORDER BY s.Month, row_count DESC