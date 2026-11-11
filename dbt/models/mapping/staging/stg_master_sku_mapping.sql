{{ config(materialized='view') }}

SELECT
    * EXCLUDE ("Key"),
    UPPER(TRIM("Key")) AS "Key"
FROM (
    SELECT *
    FROM read_xlsx(
        '{{ var("mapping_path") }}',
        header=true,
        all_varchar=true
    )
    OFFSET 1
) AS mapping_rows
