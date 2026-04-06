-- BRONZE LAYER: Uluslararası Satışlar
-- Amaç: International sales verisini temizle, para birimi standartlaştır

WITH source AS (
    SELECT * FROM "iceberg"."bronze"."raw_international_sales"
),

cleaned AS (
    SELECT
        -- Benzersiz ID oluştur (satır bazlı)
        CONCAT(
            TRIM("CUSTOMER"), '_',
            TRIM("SKU"), '_',
            CAST("DATE" AS STRING)
        ) AS transaction_id,
        
        -- Tarih dönüşümü
        DATE_PARSE("DATE", '%d-%m-%y') AS order_date,        
        -- Month (string olarak kalabilir)
        TRIM("Months") AS month_year,
        
        -- Customer (NULL kontrolü)
        COALESCE(TRIM("CUSTOMER"), 'Unknown') AS customer_name,
        
        -- Style
        TRIM("Style") AS style,
        
        -- SKU
        TRIM("SKU") AS sku,
        
        -- Size
        TRIM("Size") AS size,
        
        -- PCS (adet)
        CASE 
            WHEN "PCS" < 0 THEN 0
            WHEN "PCS" IS NULL THEN 1
            ELSE "PCS"
        END AS quantity,
        
        -- Rate (birim fiyat)
        COALESCE("RATE", 0) AS unit_price,
        
        -- Gross Amount
        COALESCE("GROSS AMT", 0) AS gross_amount,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS loaded_timestamp
        
    FROM source
)

SELECT * FROM cleaned
WHERE sku IS NOT NULL  -- SKU zorunlu