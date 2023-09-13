WITH 

source_lojas AS (

    SELECT 
         CAST(businessentityid AS INT) AS id_entidade
        ,CAST(name AS STRING) AS nome_loja
        ,CAST(salespersonid AS INT) AS id_vendedor
        --,CAST(demographics AS INT) AS
        --,CAST(rowguid AS INT) AS
        --,CAST(modifieddate AS INT) AS
    FROM {{ source('erp', 'store') }}

)

SELECT * 
FROM source_lojas
