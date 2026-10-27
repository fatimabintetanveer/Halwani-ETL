{{ config(materialized='table', schema='gold') }}

WITH distinct_products AS (
    SELECT DISTINCT
        Key as "product_key", SKU, Description, "Sub-Deptartment",
        BASKET, CATEGORY, SUB_CATEGORY, COMPANY, BRAND,
        PRODUCT_TYPE_1, PRODUCT_TYPE_2, PRODUCT_TYPE_3,
        "PACKAGING DESCRIPTION", "UOM DESCRIPTION", "SIZE DESCRIPTION", "PACK TYPE",
        PACK_QTY, GRAM, "Pack Group", PCS, "UNIT WEIGHT", "SIZE TYPE", "Drived SKU"
    FROM {{ ref('mdm_all') }}
    WHERE Key IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY product_key) AS product_id,
    *
FROM distinct_products