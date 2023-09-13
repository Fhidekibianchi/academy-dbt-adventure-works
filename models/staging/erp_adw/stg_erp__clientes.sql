WITH 

source_clientes AS (

    SELECT  
         CAST(customerid AS INT) AS id_cliente
        ,CAST(personid AS INT) AS id_pessoa
        ,CAST(storeid AS INT) AS id_loja
        ,CAST(territoryid AS INT) AS id_territorio
    FROM {{ source('erp', 'customer') }}

)

SELECT * 
FROM source_clientes
