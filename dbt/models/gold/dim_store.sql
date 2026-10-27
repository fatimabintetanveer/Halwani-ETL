{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY Retailer, "Store Num", "Store Name") AS store_id,
    Retailer,
    "Store Num",
    "Store Name",
    MAX(StoreType) AS "StoreType",
    MAX(City) AS "City",
    MAX(Region) AS "Region"
FROM {{ ref('mdm_all') }}
WHERE Retailer IS NOT NULL
GROUP BY Retailer, "Store Num", "Store Name"