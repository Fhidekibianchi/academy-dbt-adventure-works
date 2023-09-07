WITH 
source_states AS (
    SELECT 
        stateprovinceid AS id_estado
        ,territoryid AS id_territorio
        ,stateprovincecode AS codigo_estado
        ,countryregioncode AS codigo_pais
        --,isonlystateprovinceflag
        ,name AS estado        
        --,rowguid
        --,modifieddate

    FROM {{ source('erp', 'stateprovince') }}    
)

select * from source_states
