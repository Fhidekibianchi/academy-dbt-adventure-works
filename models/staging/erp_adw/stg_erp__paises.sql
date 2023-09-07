WITH
source_country AS (
    SELECT
         CAST(countryregioncode AS INT) AS codigo_pais 
        ,CAST(name AS STRING) as pais
    FROM {{ source('erp', 'countryregion') }}
)
SELECT * 
FROM source_country
