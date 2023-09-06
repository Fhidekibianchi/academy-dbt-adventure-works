WITH
    source_address AS (
        SELECT 
            addressid AS id_endereco					
            --,addressline1						
            --,addressline2						
            ,city AS cidade	
            ,stateprovinceid AS id_estado						
            --,postalcode						
            --,spatiallocation						
            --,rowguid						
            --,modifieddate		
        FROM {{ source('erp', 'address') }}
    )
SELECT *
FROM source_address