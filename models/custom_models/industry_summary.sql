{{ config(
    materialized='table'
) }}

WITH aggregated_data AS (
    SELECT 
        YEAR,
        INDUSTRY_CODE_ANZSIC AS industry_code,
        INDUSTRY_NAME_ANZSIC AS industry_name,
        COUNT(*) AS record_count,
        SUM(TRY_TO_NUMERIC(VALUE)) AS total_value,
        AVG(TRY_TO_NUMERIC(VALUE)) AS average_value
    FROM {{ source('RAW_SOURCE', 'DATA_SAMPLE') }}
    GROUP BY 
        YEAR, 
        INDUSTRY_CODE_ANZSIC, 
        INDUSTRY_NAME_ANZSIC
)
SELECT *
FROM aggregated_data
