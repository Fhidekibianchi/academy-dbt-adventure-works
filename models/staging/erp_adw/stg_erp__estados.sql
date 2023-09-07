WITH 
source_states AS (
    SELECT 
         CAST(stateprovinceid AS INT) AS id_estado
        ,CAST(territoryid AS INT) AS id_territorio
        ,CAST(stateprovincecode AS STRING) AS codigo_estado
        ,CAST(countryregioncode AS STRING) AS codigo_pais
        --isonlystateprovinceflag
        ,CAST(name AS STRING) AS estado        
        --,rowguid
        --,modifieddate

    FROM {{ source('erp', 'stateprovince') }}    
)

select * from source_states
