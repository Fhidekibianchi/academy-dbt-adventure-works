WITH
    source_address AS (
        SELECT 
             CAST(addressid AS INT) AS id_endereco					
            ,CAST(stateprovinceid AS INT) AS id_estado
            ,CAST(addressline1 AS STRING) AS endereco						
            --,addressline2						
            ,CAST(city AS STRING) AS cidade	       						
            --,postalcode						
            --,spatiallocation						
            --,rowguid						
            --,modifieddate		
        FROM {{ source('erp', 'address') }}
    )
SELECT *
FROM source_address