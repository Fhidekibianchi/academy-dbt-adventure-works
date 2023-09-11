WITH 

source_motivo AS (
    SELECT 
         CAST(salesreasonid AS INT) AS id_motivo_compra
        ,CAST(name AS STRING) AS motivo_compra 
        ,CAST(reasontype AS STRING) AS tipo_motivo_compra 
        --,CAST(modifieddate AS INT) AS 
    FROM {{ source('erp', 'salesreason') }}
    
)
SELECT * 
FROM source_motivo
