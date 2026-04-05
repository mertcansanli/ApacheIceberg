-- SILVER LAYER: Ürün Boyut Tablosu
-- Amaç: Ürün master verisini oluştur
-- Benzersiz: sku_code bazlı

WITH stock_data AS (
    SELECT * FROM {{ ref('stg_stock_levels') }}
),

amazon_data AS (
    SELECT DISTINCT sku, category, size 
    FROM {{ ref('stg_amazon_sales') }}
    WHERE sku != 'Unknown'
),

international_data AS (
    SELECT DISTINCT sku, style 
    FROM {{ ref('stg_international_sales') }}
    WHERE sku != 'Unknown'
),

-- Ürün master oluştur
product_master AS (
    SELECT DISTINCT
        COALESCE(s.sku_code, a.sku, i.sku) AS product_sku,
        
        -- Design number
        s.design_number,
        
        -- Product name (SKU'dan türet)
        CASE 
            WHEN s.sku_code LIKE 'AN%' THEN 'Leggings'
            WHEN a.sku LIKE 'SET%' THEN 'Set'
            WHEN a.sku LIKE 'JNE%' THEN 'Kurta'
            WHEN a.sku LIKE 'MEN%' THEN 'Men Kurta'
            WHEN i.style LIKE 'BL%' THEN 'Blouse'
            WHEN i.style LIKE 'J%' THEN 'Jacket'
            ELSE 'Other'
        END AS product_category,
        
        -- Style (international'dan)
        i.style,
        
        -- Size
        COALESCE(s.size, a.size, i.size, 'Unknown') AS size,
        
        -- Color (sadece stock'ta var)
        s.color,
        
        -- Stock'dan gelen bilgiler
        s.category AS stock_category,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS updated_timestamp
        
    FROM stock_data s
    FULL OUTER JOIN amazon_data a ON s.sku_code = a.sku
    FULL OUTER JOIN international_data i ON s.sku_code = i.sku
)

SELECT * FROM product_master
WHERE product_sku IS NOT NULL