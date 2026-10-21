{% macro sku_mapping() %}
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
{% endmacro %}