{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        TRY_CAST(TRIM("Src_Store Num") AS INTEGER) AS "Src_Store Num",
        TRIM("Src_Store Name") AS "Src_Store Name",
        TRIM("Src_StoreType") AS "Src_StoreType",
        TRIM("Src_City") AS "Src_City",
        TRIM("Src_Region") AS "Src_Region",
        TRIM("Mapped_Store Name") AS "Store Name",
        TRIM("Mapped_StoreType") AS "StoreType",
        TRIM("Mapped_City") AS "City",
        TRIM("Mapped_Region") AS "Region",
        ROW_NUMBER() OVER (
            PARTITION BY
                TRY_CAST(TRIM("Src_Store Num") AS INTEGER),
                TRIM("Src_Store Name")
            ORDER BY "Mapped_Store Name"
        ) AS rn
    FROM read_xlsx(
        '{{ var("mapping_path") }}',
        sheet='Store',
        header=true,
        all_varchar=true
    )
)
SELECT
    "Src_Store Num", "Src_Store Name", "Src_StoreType",
    "Src_City", "Src_Region", "Store Name", "StoreType", "City", "Region"
FROM ranked
WHERE rn = 1
ORDER BY "Src_Store Num", "Src_Store Name"