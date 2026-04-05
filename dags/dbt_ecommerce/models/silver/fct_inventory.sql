-- SILVER LAYER: Envanter Faktus Tablosu
-- Amaç: Stok hareketlerini takip et

WITH stock_snapshots AS (
    SELECT
        sku_code AS product_sku,
        stock_quantity,
        category,
        size,
        color,
        loaded_timestamp AS snapshot_date
    FROM {{ ref('stg_stock_levels') }}
),

-- Önceki stok değerini hesapla (değişim analizi için)
inventory_with_prev AS (
    SELECT
        *,
        LAG(stock_quantity) OVER (
            PARTITION BY product_sku 
            ORDER BY snapshot_date
        ) AS previous_stock_quantity,
        
        -- Stok değişimi
        stock_quantity - LAG(stock_quantity) OVER (
            PARTITION BY product_sku 
            ORDER BY snapshot_date
        ) AS stock_change
        
    FROM stock_snapshots
),

-- Stok seviyesi kategorizasyonu
inventory_categorized AS (
    SELECT
        *,
        CASE 
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_quantity <= 5 THEN 'Critical Stock'
            WHEN stock_quantity <= 20 THEN 'Low Stock'
            WHEN stock_quantity <= 50 THEN 'Normal Stock'
            ELSE 'Excess Stock'
        END AS stock_status,
        
        -- Yeniden sipariş önerisi
        CASE 
            WHEN stock_quantity <= 5 THEN 'Reorder Immediately'
            WHEN stock_quantity <= 20 THEN 'Reorder Soon'
            ELSE 'Stock Sufficient'
        END AS reorder_suggestion
        
    FROM inventory_with_prev
)

SELECT * FROM inventory_categorized