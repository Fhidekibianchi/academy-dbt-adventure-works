WITH 
source_subcategoria_produto AS (
    SELECT
         CAST(productsubcategoryid AS INT) AS id_subcategoria_produto
        ,CAST(productcategoryid AS INT) AS id_categoria_produto
        ,CAST(name AS STRING) AS subcategoria_produto
        --,rowguid
        --,modifieddate 
    FROM {{ source('erp', 'productsubcategory') }}
)

SELECT *
FROM source_subcategoria_produto
