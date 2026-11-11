WITH source_totals AS (
    SELECT
        Retailer,
        Month,
        UPPER(TRIM("Key")) AS join_key,
        SUM(TRY_CAST("Sales Amount" AS DOUBLE)) AS source_amount,
        SUM(TRY_CAST("Sales Quantity" AS DOUBLE)) AS source_quantity
    FROM {{ source('silver', 'silver_unified') }}
    GROUP BY Retailer, Month, UPPER(TRIM("Key"))
),
joined_totals AS (
    SELECT
        s.Retailer,
        s.Month,
        UPPER(TRIM(s."Key")) AS join_key,
        SUM(TRY_CAST(s."Sales Amount" AS DOUBLE)) AS joined_amount,
        SUM(TRY_CAST(s."Sales Quantity" AS DOUBLE)) AS joined_quantity
    FROM {{ source('silver', 'silver_unified') }} AS s
    LEFT JOIN {{ ref('master_sku_mapping') }} AS m
        ON UPPER(TRIM(s."Key")) = UPPER(m."Key")
    WHERE TRY_CAST(NULLIF(TRIM(m."Active/Inactive"), '') AS INTEGER) != 1
       OR TRY_CAST(NULLIF(TRIM(m."Active/Inactive"), '') AS INTEGER) IS NULL
    GROUP BY s.Retailer, s.Month, UPPER(TRIM(s."Key"))
)
SELECT
    s.Retailer,
    s.Month,
    s.join_key,
    s.source_amount,
    j.joined_amount,
    s.source_quantity,
    j.joined_quantity
FROM source_totals AS s
JOIN joined_totals AS j
    ON s.Retailer = j.Retailer
    AND s.Month = j.Month
    AND s.join_key IS NOT DISTINCT FROM j.join_key
WHERE ABS(COALESCE(s.source_amount, 0) - COALESCE(j.joined_amount, 0)) > 0.01
   OR ABS(COALESCE(s.source_quantity, 0) - COALESCE(j.joined_quantity, 0)) > 0.000001
