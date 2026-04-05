-- GOLD LAYER: Envanter Analizi
-- Amaç: Stok sağlığı ve optimizasyonu

WITH inventory_data AS (
    SELECT * FROM {{ ref('fct_inventory') }}
),

sales_data AS (
    SELECT 
        product_sku,
        SUM(quantity) AS total_sold_30days
    FROM {{ ref('fct_sales') }}
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY product_sku
),

inventory_health AS (
    SELECT
        i.product_sku,
        i.category,
        i.size,
        i.color,
        i.stock_quantity,
        i.stock_status,
        i.reorder_suggestion,
        i.snapshot_date,
        
        COALESCE(s.total_sold_30days, 0) AS units_sold_30days,
        
        -- Günlük satış hızı
        COALESCE(s.total_sold_30days, 0) / 30.0 AS daily_sales_rate,
        
        -- Stok gün sayısı (mevcut stok kaç gün yeter)
        CASE 
            WHEN COALESCE(s.total_sold_30days, 0) > 0 
            THEN i.stock_quantity / (s.total_sold_30days / 30.0)
            ELSE 999
        END AS days_of_inventory,
        
        -- Stok değişim trendi
        AVG(i.stock_change) OVER (
            PARTITION BY i.product_sku 
            ORDER BY i.snapshot_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_weekly_stock_change
        
    FROM inventory_data i
    LEFT JOIN sales_data s ON i.product_sku = s.product_sku
),

-- Öneriler
final_analysis AS (
    SELECT
        *,
        CASE 
            WHEN days_of_inventory < 7 THEN 'CRITICAL - Immediate Reorder Required'
            WHEN days_of_inventory < 14 THEN 'URGENT - Reorder This Week'
            WHEN days_of_inventory < 30 THEN 'NORMAL - Reorder Soon'
            WHEN days_of_inventory > 90 THEN 'WARNING - Excess Inventory'
            ELSE 'HEALTHY - Stock Level Optimal'
        END AS inventory_action_required,
        
        -- Tahmini yeniden sipariş miktarı
        CASE 
            WHEN days_of_inventory < 14 
            THEN CEIL(daily_sales_rate * 30) - stock_quantity
            ELSE 0
        END AS suggested_reorder_quantity
        
    FROM inventory_health
)

SELECT * FROM final_analysis
WHERE stock_quantity > 0 OR units_sold_30days > 0
ORDER BY days_of_inventory ASC