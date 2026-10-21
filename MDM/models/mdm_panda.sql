{{ config(materialized='ephemeral') }}

WITH panda_key AS (
    SELECT
        'Panda' || SKU || CASE WHEN SizeDesc = '' OR SizeDesc IS NULL THEN '' ELSE '-' || SizeDesc END AS Key,
        s.*
    FROM {{ source('silver', 'silver_panda') }} s
),

sku_mapping AS ( {{ sku_mapping() }} )

SELECT
    s.Key,
    s.Month,
    m.Description,
    m."Sub-Deptartment",
    'Panda' AS Retailer,
    m.BASKET, m.CATEGORY, m.SUB_CATEGORY, m.COMPANY, m.BRAND,
    TRY_CAST(s.SKU AS INTEGER) AS SKU,
    m.PRODUCT_TYPE_1, m.PRODUCT_TYPE_2, m.PRODUCT_TYPE_3,
    m."PACKAGING DESCRIPTION", m."UOM DESCRIPTION", m."SIZE DESCRIPTION", m."PACK TYPE",
    m.PACK_QTY, m.GRAM, m."Pack Group", m.PCS, m."UNIT WEIGHT", m."SIZE TYPE",
    m."Drived SKU",
    TRY_CAST(s."Sales Quantity" AS DOUBLE) AS Sales_Quantity,
    TRY_CAST(s."Sales Amount" AS DOUBLE) AS Sales_Amount,
    TRY_CAST(s."Store Num" AS INTEGER) AS Store_Num,
    s.StoreType,
    s."Store Name" AS Store_Name,
    s."City" AS City,
    s."Region" AS Region,
    TRY_CAST(s."Promo Week" AS INTEGER) AS Promo_Week,
    s."Promo Offer Code" AS Promo_Offer_Code
FROM panda_key s
LEFT JOIN sku_mapping m ON UPPER(s.Key) = UPPER(m.Key)
WHERE m.Active_Inactive != 1 OR m.Active_Inactive IS NULL