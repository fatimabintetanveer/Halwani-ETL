{{ config(materialized='table', alias='master_sku_mapping') }}

SELECT DISTINCT *
FROM read_xlsx(
    '{{ var("mapping_path") }}',
    header=true,
    all_varchar=true
)
WHERE Key IS NOT NULL AND Key != '0'