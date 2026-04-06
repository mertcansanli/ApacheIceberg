-- GOLD LAYER: Ürün Bazında Satış Özeti
-- Amaç: Her ürün için KPI'lar

WITH sales_data AS (
    SELECT * FROM "iceberg"."bronze_silver"."fct_sales"
),

product_sales AS (
    SELECT
        product_sku,
        product_category,
        size,
        color,
        sales_channel,
        
        -- Satış metrikleri
        COUNT(DISTINCT sales_id) AS total_transactions,
        SUM(quantity) AS total_units_sold,
        SUM(gross_revenue_inr) AS total_revenue_inr,
        AVG(unit_price) AS avg_selling_price,
        
        -- Zaman bazlı
        COUNT(DISTINCT order_year_month) AS active_months,
        MIN(order_date) AS first_sale_date,
        MAX(order_date) AS last_sale_date,
        
        -- Ortalama sipariş değeri
        SUM(gross_revenue_inr) / NULLIF(COUNT(DISTINCT sales_id), 0) AS avg_order_value
        
    FROM sales_data
    WHERE order_status NOT IN ('Cancelled')
    GROUP BY 1, 2, 3, 4, 5
),

-- Performans sınıflandırması
ranked_products AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_revenue_inr DESC) AS revenue_rank,
        RANK() OVER (ORDER BY total_units_sold DESC) AS volume_rank,
        
        CASE 
            WHEN total_revenue_inr > 100000 THEN 'Premium'
            WHEN total_revenue_inr > 50000 THEN 'High'
            WHEN total_revenue_inr > 10000 THEN 'Medium'
            ELSE 'Low'
        END AS revenue_tier
        
    FROM product_sales
)

SELECT * FROM ranked_products
ORDER BY total_revenue_inr DESC