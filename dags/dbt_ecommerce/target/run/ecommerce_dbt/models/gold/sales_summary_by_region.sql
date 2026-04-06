
  
    

    create table "iceberg"."bronze_gold"."sales_summary_by_region"
      
      
    as (
      -- GOLD LAYER: Bölge Bazında Satış Özeti
-- Amaç: Coğrafi satış performansı

WITH sales_data AS (
    SELECT * FROM "iceberg"."bronze_silver"."fct_sales"
),

regional_sales AS (
    SELECT
        ship_state,
        ship_city,
        ship_country,
        sales_channel,
        
        -- Satış metrikleri
        COUNT(DISTINCT sales_id) AS total_orders,
        SUM(quantity) AS total_units,
        SUM(gross_revenue_inr) AS total_revenue_inr,
        AVG(unit_price) AS avg_price,
        
        -- Müşteri segmentasyonu
        SUM(CASE WHEN is_b2b THEN gross_revenue_inr ELSE 0 END) AS b2b_revenue,
        SUM(CASE WHEN NOT is_b2b THEN gross_revenue_inr ELSE 0 END) AS b2c_revenue,
        
        -- B2B oranı
        ROUND(
            100.0 * SUM(CASE WHEN is_b2b THEN gross_revenue_inr ELSE 0 END) / 
            NULLIF(SUM(gross_revenue_inr), 0), 2
        ) AS b2b_percentage
        
    FROM sales_data
    WHERE order_status NOT IN ('Cancelled')
        AND ship_state IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- Toplam satış içindeki pay
total_sales AS (
    SELECT SUM(total_revenue_inr) AS grand_total FROM regional_sales
)

SELECT
    r.*,
    ROUND(100.0 * r.total_revenue_inr / t.grand_total, 2) AS revenue_share_percentage,
    RANK() OVER (ORDER BY r.total_revenue_inr DESC) AS state_rank
FROM regional_sales r
CROSS JOIN total_sales t
ORDER BY total_revenue_inr DESC
    );

  