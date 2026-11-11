SELECT "Key"
FROM {{ ref('stg_master_sku_mapping') }}
WHERE "Key" IS NULL
   OR TRIM("Key") = ''
   OR TRIM("Key") = '0'
