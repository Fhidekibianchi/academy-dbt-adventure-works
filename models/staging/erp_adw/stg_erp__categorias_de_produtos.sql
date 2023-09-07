WITH 
source_categoria_produto AS  (

    SELECT
         CAST(productcategoryid AS INT) AS id_categoria_produto
        ,CAST(name AS STRING) AS categoria_produto
        --,rowguid
        --,modifieddate
    FROM {{ source('erp', 'productcategory') }}

)
SELECT * 
FROM source_categoria_produto
