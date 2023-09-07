WITH
    source_address AS (
        SELECT 
            addressid AS id_endereco					
            ,stateprovinceid AS id_estado
            ,addressline1 AS endereco						
            --,addressline2						
            ,city AS cidade	       						
            --,postalcode						
            --,spatiallocation						
            --,rowguid						
            --,modifieddate		
        FROM {{ source('erp', 'address') }}
    )
SELECT *
FROM source_address