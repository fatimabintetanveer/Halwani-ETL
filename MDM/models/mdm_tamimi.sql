{{ config(materialized='ephemeral') }}

WITH tamimi_key AS (
    SELECT
        'Tamimi' || SKU || "Sales Unit" || CASE WHEN SizeDesc = '' OR SizeDesc IS NULL THEN '' ELSE '-' || SizeDesc END AS Key,
        s.*
    FROM {{ source('silver', 'silver_tamimi') }} s
),

sku_mapping AS ( {{ sku_mapping() }} )

SELECT
    s.Key,
    s.Month,
    m.Description,
    m."Sub-Deptartment",
    'Tamimi' AS Retailer,
    m.BASKET, m.CATEGORY, m.SUB_CATEGORY, m.COMPANY, m.BRAND,
    s.SKU,
    m.PRODUCT_TYPE_1, m.PRODUCT_TYPE_2, m.PRODUCT_TYPE_3,
    m."PACKAGING DESCRIPTION", m."UOM DESCRIPTION", m."SIZE DESCRIPTION", m."PACK TYPE",
    m.PACK_QTY, m.GRAM, m."Pack Group", m.PCS, m."UNIT WEIGHT", m."SIZE TYPE",
    m."Drived SKU",
    TRY_CAST(s."Sales Quantity" AS DOUBLE) AS Sales_Quantity,
    TRY_CAST(s."Sales Amount" AS DOUBLE) AS Sales_Amount,
    CAST(NULL AS INTEGER) AS Store_Num,
    CAST(NULL AS STRING) AS StoreType,
    CAST(NULL AS STRING) AS Store_Name,
    CAST(NULL AS STRING) AS City,
    CAST(NULL AS STRING) AS Region,
    CAST(NULL AS INTEGER) AS Promo_Week,
    CAST(NULL AS STRING) AS Promo_Offer_Code
FROM tamimi_key s
LEFT JOIN sku_mapping m ON UPPER(s.Key) = UPPER(m.Key)
WHERE m.Active_Inactive != 1 OR m.Active_Inactive IS NULL