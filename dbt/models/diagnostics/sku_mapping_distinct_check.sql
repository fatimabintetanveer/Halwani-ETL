{{ config(materialized='table') }}

-- Temporary diagnostic model: distinct rows imported directly from the SKU
-- mapping workbook after skipping its non-data second row.
SELECT DISTINCT *
FROM read_xlsx(
    '{{ var("mapping_path") }}',
    header=true,
    all_varchar=true
)
OFFSET 1
