-- SILVER LAYER: Satış Faktus Tablosu
-- Amaç: Tüm satışları birleştir, ilişkilendir

WITH amazon_sales AS (
    SELECT
        order_id AS sales_id,
        order_date,
        order_status,
        sku,
        quantity,
        amount,
        currency,
        ship_city,
        ship_state,
        ship_country,
        is_b2b,
        'AMAZON' AS sales_channel
    FROM {{ ref('stg_amazon_sales') }}
    WHERE order_status NOT IN ('Cancelled')  -- İptal edilenleri filtrele
),

international_sales AS (
    SELECT
        transaction_id AS sales_id,
        order_date,
        'Completed' AS order_status,  -- International hep tamamlandı varsay
        sku,
        quantity,
        gross_amount AS amount,
        'USD' AS currency,  -- International USD varsay
        NULL AS ship_city,
        NULL AS ship_state,
        'International' AS ship_country,
        FALSE AS is_b2b,
        'INTERNATIONAL' AS sales_channel
    FROM {{ ref('stg_international_sales') }}
),

-- Tüm satışları birleştir
all_sales AS (
    SELECT * FROM amazon_sales
    UNION ALL
    SELECT * FROM international_sales
),

-- Ürün ve müşteri bilgilerini join'le
enriched_sales AS (
    SELECT
        s.sales_id,
        s.order_date,
        s.order_status,
        p.product_sku,
        p.product_category,
        p.size,
        p.color,
        s.quantity,
        s.amount,
        s.amount / NULLIF(s.quantity, 0) AS unit_price,
        s.currency,
        s.ship_city,
        s.ship_state,
        s.ship_country,
        s.is_b2b,
        s.sales_channel,
        
        -- Revenue hesaplamaları
        s.amount AS gross_revenue,
        CASE 
            WHEN s.currency = 'USD' THEN s.amount * 83.5  -- USD to INR
            ELSE s.amount
        END AS gross_revenue_inr,
        
        -- Ay ve yıl ekstraksiyonu
        EXTRACT(YEAR FROM s.order_date) AS order_year,
        EXTRACT(MONTH FROM s.order_date) AS order_month,
        FORMAT_DATE('%Y-%m', s.order_date) AS order_year_month
        
    FROM all_sales s
    LEFT JOIN {{ ref('dim_products') }} p ON s.sku = p.product_sku
)

SELECT * FROM enriched_sales
WHERE sales_id IS NOT NULL