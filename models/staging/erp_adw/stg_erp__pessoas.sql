WITH 

source_pessoas AS (

    SELECT 
         CAST(businessentityid AS INT) AS id_entidade
        ,CAST(persontype AS STRING) AS tipo_pessoa
        --,CAST(namestyle AS BOOL) AS  
        ,CAST(title AS STRING) AS forma_tratamento
        ,CAST(firstname AS STRING) AS primeiro_nome
        ,CAST(middlename AS STRING) AS nome_meio
        ,CAST(lastname AS STRING) AS sobrenome
        ,CAST(suffix AS STRING) AS sufixo 
    FROM {{ source('erp', 'person') }}

)
SELECT * 
FROM source_pessoas
