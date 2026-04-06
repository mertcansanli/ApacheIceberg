-- BRONZE LAYER: Amazon Satışları
-- Amaç: Amazon satış verisini temizle, tarih formatını düzelt

WITH source AS (
    SELECT * FROM "iceberg"."bronze"."raw_amazon_sales"
),

cleaned AS (
    SELECT
        -- Order ID (birincil anahtar)
        TRIM("Order ID") AS order_id,
        
        -- Tarih dönüşümü (DD-MM-YY -> DATE)
        
        DATE_PARSE("Date", '%d-%m-%y') AS order_date,
        
        -- Status (NULL kontrolü)
        COALESCE(TRIM("Status"), 'Unknown') AS order_status,
        
        -- Fulfilment tipi
        TRIM("Fulfilment") AS fulfilment_type,
        
        -- Satış kanalı
        TRIM("Sales Channel ") AS sales_channel,
        
        -- SKU (NULL ise 'Unknown')
        COALESCE(TRIM("SKU"), 'Unknown') AS sku,
        
        -- Kategori
        TRIM("Category") AS category,
        
        -- Size
        TRIM("Size") AS size,
        
        -- Quantity (negatif veya null ise 0)
        CASE 
            WHEN "Qty" < 0 THEN 0
            WHEN "Qty" IS NULL THEN 0
            ELSE "Qty"
        END AS quantity,
        
        -- Amount (NULL ise 0)
        COALESCE("Amount", 0) AS amount,
        
        -- Currency
        COALESCE(TRIM("currency"), 'INR') AS currency,
        
        -- Location bilgileri
        TRIM("ship-city") AS ship_city,
        TRIM("ship-state") AS ship_state,
        "ship-postal-code" AS ship_postal_code,
        TRIM("ship-country") AS ship_country,
        
        -- B2B flag
        COALESCE("B2B", FALSE) AS is_b2b,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS loaded_timestamp
        
    FROM source
)

SELECT * FROM cleaned
WHERE order_id IS NOT NULL  -- Order ID zorunlu