{{ config(materialized='table', schema='gold') }}

SELECT
    p.product_id,
    COALESCE(s.store_id, -1) AS store_id,
    f.Month,
    f."Sales Quantity",
    f."Sales Amount",
    f."Promo Week",
    f."Promo Offer Code",
FROM {{ ref('mdm_all') }} f
LEFT JOIN {{ ref('dim_product') }} p ON f.Key = p.product_key
LEFT JOIN {{ ref('dim_store') }} s
    ON f.Retailer = s.Retailer
    AND f."Store Num" = s."Store Num"
    AND f."Store Name" = s."Store Name"
