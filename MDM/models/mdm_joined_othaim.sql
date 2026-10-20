{{ config(materialized='table') }}

SELECT
    'Othaim' || SKU || '-' || SizeDesc AS key,
    s.*,
    m.BASKET,
    m.CATEGORY,
    m.SUB_CATEGORY,
    m.COMPANY,
    m.BRAND,
    m.GRAM,
    m.PRODUCT_TYPE_1,
    m.PRODUCT_TYPE_2,
    m.PRODUCT_TYPE_3,
    m.PACK_QTY,
    CASE WHEN m.Key IS NULL THEN 'unmapped' ELSE 'mapped' END AS mapping_status
FROM {{ source('silver', 'silver_othaim') }} s
LEFT JOIN {{ ref('master_sku_mapping') }} m
    ON 'Othaim' || s.SKU || '-' || s.SizeDesc = m.Key