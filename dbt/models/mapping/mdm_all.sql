{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['Key', 'Month', 'Retailer'],
    alias='harmonized_mapping'
) }}

{% set month = var('month', none) %}
{% set retailer = var('retailer', none) %}

WITH raw AS (
    SELECT
        s.*,
        COALESCE(sm."Store Name", s."Store Name") AS mapped_store_name,
        COALESCE(sm."StoreType", s."StoreType") AS mapped_store_type,
        COALESCE(sm."City", s."City") AS mapped_city,
        COALESCE(sm."Region", s."Region") AS mapped_region
    FROM {{ source('silver', 'silver_unified') }} s
    LEFT JOIN {{ ref('master_store_mapping') }} sm
        ON s.Retailer = 'Panda'
        AND TRY_CAST(s."Store Num" AS INTEGER) = sm."Src_Store Num"
        AND REGEXP_REPLACE(UPPER(s."Store Name"), '[^A-Z0-9]', '', 'g') = REGEXP_REPLACE(UPPER(sm."Src_Store Name"), '[^A-Z0-9]', '', 'g')
),

sku_mapping AS(
    SELECT
        Key,
        CAST(NULLIF(TRIM(Sku), '') AS INTEGER) AS Sku,
        Description,
        Retailer,
        "Sub-Deptartment",
        BASKET, CATEGORY, SUB_CATEGORY, COMPANY, BRAND,
        PRODUCT_TYPE_1, PRODUCT_TYPE_2, PRODUCT_TYPE_3,
        "PACKAGING DESCRIPTION", "UOM DESCRIPTION", "SIZE DESCRIPTION", "PACK TYPE",
        CAST(NULLIF(TRIM(PACK_QTY), '') AS INTEGER) AS PACK_QTY,
        CAST(NULLIF(TRIM(GRAM), '') AS INTEGER) AS GRAM,
        "Pack Group",
        CAST(NULLIF(TRIM(PCS), '') AS INTEGER) AS PCS,
        "UNIT WEIGHT", "SIZE TYPE",
        "Drived SKU",
        CAST(NULLIF(TRIM("Active/Inactive"), '') AS INTEGER) AS Active_Inactive
    FROM {{ ref('master_sku_mapping') }}
)

SELECT
    s.Key,
    s.Month,
    m.Retailer,
    m.Description,
    m."Sub-Deptartment",
    m.BASKET, m.CATEGORY, m.SUB_CATEGORY, m.COMPANY, m.BRAND,
    TRY_CAST(s.SKU AS INTEGER) AS SKU,
    m.PRODUCT_TYPE_1, m.PRODUCT_TYPE_2, m.PRODUCT_TYPE_3,
    m."PACKAGING DESCRIPTION", m."UOM DESCRIPTION", m."SIZE DESCRIPTION", m."PACK TYPE",
    m.PACK_QTY, m.GRAM, m."Pack Group", m.PCS, m."UNIT WEIGHT", m."SIZE TYPE",
    m."Drived SKU",
    SUM(TRY_CAST(s."Sales Quantity" AS DOUBLE)) AS "Sales Quantity",
    SUM(TRY_CAST(s."Sales Amount" AS DOUBLE)) AS "Sales Amount",
    TRY_CAST(s."Store Num" AS INTEGER) AS "Store Num",
    s.mapped_store_type AS StoreType,
    s.mapped_store_name AS "Store Name",
    s.mapped_city AS City,
    s.mapped_region AS Region,
    TRY_CAST(s."Promo Week" AS INTEGER) AS "Promo Week",
    s."Promo Offer Code" AS "Promo Offer Code"
FROM raw s
LEFT JOIN sku_mapping m
    ON UPPER(TRIM(s.Key)) = UPPER(m.Key)
WHERE m.Active_Inactive != 1 OR m.Active_Inactive IS NULL
    {% if is_incremental() %}
        {% if month %}AND s.Month = '{{ month }}'{% endif %}
        {% if retailer %}AND s.Retailer = '{{ retailer }}'{% endif %}
    {% endif %}
GROUP BY ALL