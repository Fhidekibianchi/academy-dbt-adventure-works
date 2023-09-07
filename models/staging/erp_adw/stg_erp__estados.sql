WITH 
source_states AS (
    SELECT 
        stateprovinceid AS id_estado
        ,stateprovincecode AS codigo_estado
        ,countryregioncode AS codigo_pais
        --,isonlystateprovinceflag
        ,name AS estado
        ,territoryid AS id_territorio
        --,rowguid
        --,modifieddate

    FROM {{ source('erp', 'stateprovince') }}    
)

select * from source_states
