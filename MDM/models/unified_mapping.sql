{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['Key', 'Month', 'Retailer'],
    alias='unified_mapping'
) }}

{% set retailer = var('retailer', 'all') %}
{% set month = var('month', 'all') %}
{% set ns = namespace(first=true) %}

{% for r in var('retailers') %}
    {% if retailer == 'all' or retailer == r %}
        {% if not ns.first %}UNION ALL{% endif %}
        SELECT '{{ r }}' AS Retailer, *
        FROM {{ ref('mdm_' ~ r | lower) }}
        {% if month != 'all' %}WHERE Month = '{{ month }}'{% endif %}
        {% set ns.first = false %}
    {% endif %}
{% endfor %}