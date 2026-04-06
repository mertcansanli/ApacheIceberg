-- SILVER LAYER: Müşteri Boyut Tablosu
-- Amaç: Benzersiz müşteri listesi oluştur

WITH amazon_customers AS (
    SELECT DISTINCT
        'AMAZON' AS source_system,
        order_id AS transaction_id,
        ship_city AS city,
        ship_state AS state,
        ship_country AS country,
        is_b2b AS is_business_customer
    FROM "iceberg"."bronze_bronze"."stg_amazon_sales"
),

international_customers AS (
    SELECT DISTINCT
        'INTERNATIONAL' AS source_system,
        customer_name AS customer_name,
        NULL AS city,
        NULL AS state,
        'International' AS country,
        FALSE AS is_business_customer
    FROM "iceberg"."bronze_bronze"."stg_international_sales"
),

-- Müşteri master
customer_master AS (
    SELECT
        MD5(CONCAT(
            COALESCE(ac.transaction_id, ''),
            COALESCE(ic.customer_name, '')
        )) AS customer_id,
        
        ic.customer_name,
        ac.transaction_id AS amazon_order_id,
        ac.city,
        ac.state,
        ac.country,
        ac.is_business_customer,
        ic.source_system
        
    FROM amazon_customers ac
    FULL OUTER JOIN international_customers ic ON 1=0  -- Cross join mantığı
)

SELECT * FROM customer_master