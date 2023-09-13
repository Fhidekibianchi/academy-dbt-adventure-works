WITH 

source_cartao AS (

    SELECT 
         CAST(creditcardid AS INT) AS id_cartao_credito
        ,CAST(cardtype AS STRING) AS tipo_cartao
    FROM {{ source('erp', 'creditcard') }}

)

SELECT *
FROM source_cartao