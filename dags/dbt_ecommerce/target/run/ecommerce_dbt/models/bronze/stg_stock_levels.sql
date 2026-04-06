
  
    

    create table "iceberg"."bronze_bronze"."stg_stock_levels"
      
      
    as (
      -- BRONZE LAYER: Stok Seviyeleri
-- Amaç: Ham stock verisini temizle, standartlaştır
-- Data Quality: Null kontrolü, veri tipi dönüşümü

WITH source AS (
    SELECT * FROM "iceberg"."bronze"."raw_stock_levels"
),

cleaned AS (
    SELECT
        -- SKU kodunu temizle (boşlukları kaldır)
        TRIM("SKU Code") AS sku_code,
        
        -- Design numarasını ayıkla
        TRIM("Design No.") AS design_number,
        
        -- Stock miktarı (negatif değerleri 0 yap)
        CASE 
            WHEN "Stock" < 0 THEN 0
            WHEN "Stock" IS NULL THEN 0
            ELSE "Stock"
        END AS stock_quantity,
        
        -- Kategori (default değer ver)
        COALESCE(TRIM("Category"), 'Unknown') AS category,
        
        -- Size (NULL ise 'Unknown')
        COALESCE(TRIM("Size"), 'Unknown') AS size,
        
        -- Color (NULL ise 'Unknown')
        COALESCE(TRIM("Color"), 'Unknown') AS color,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS loaded_timestamp
        
    FROM source
)

SELECT * FROM cleaned
WHERE sku_code IS NOT NULL  -- SKU zorunlu
    );

  