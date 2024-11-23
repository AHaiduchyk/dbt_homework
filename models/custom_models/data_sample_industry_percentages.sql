{{ config(
    materialized='table'
) }}

WITH exclude_data AS (
    SELECT YEAR, INDUSTRY_CODE
    FROM {{ ref('exclude_data_sample') }}
),

industry_totals AS (
    SELECT 
        YEAR,
        SUM(total_value) AS total_value_all_industries  -- Total value across all industries for the year
    FROM {{ ref('industry_summary') }}
    WHERE (YEAR, INDUSTRY_CODE) NOT IN (SELECT YEAR, INDUSTRY_CODE FROM exclude_data)
    GROUP BY YEAR
),

industry_percentage AS (
    SELECT 
        i.YEAR,
        i.industry_code,
        i.industry_name,
        i.record_count,
        i.total_value,
        i.average_value,
        t.total_value_all_industries,
        ROUND((i.total_value / t.total_value_all_industries) * 100, 2) AS percent_of_total -- Percentage contribution
    FROM {{ ref('industry_summary') }} i
    LEFT JOIN exclude_data e
        ON i.YEAR = e.YEAR AND i.industry_code = e.INDUSTRY_CODE
    JOIN industry_totals t
        ON i.YEAR = t.YEAR
    WHERE e.YEAR IS NULL  -- Exclude rows in the `exclude_data` seed
)

SELECT 
    YEAR,
    industry_code,
    industry_name,
    record_count,
    total_value,
    average_value,
    percent_of_total  
FROM industry_percentage
ORDER BY YEAR, percent_of_total DESC
