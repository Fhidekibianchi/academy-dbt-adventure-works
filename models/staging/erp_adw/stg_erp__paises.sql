WITH
source_country AS (
    SELECT
        countryregioncode AS codigo_pais 
        ,name as pais
    FROM {{ source('erp', 'countryregion') }}
)
SELECT * 
FROM source_country
