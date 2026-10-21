{{ config(
    materialized='external',
    location='data/exports/unmapped_keys',
    options={'format': 'csv', 'partition_by': 'Retailer', 'overwrite': True}
) }}

{% set retailer = var('retailer', 'all') %}
{% set ns = namespace(first=true) %}

{% for r in var('retailers') %}
    {% if retailer == 'all' or retailer == r %}
        {% if not ns.first %}UNION ALL{% endif %}
        SELECT '{{ r }}' AS Retailer, *
        FROM {{ ref('mdm_unmapped_' ~ r | lower) }}
        {% set ns.first = false %}
    {% endif %}
{% endfor %}