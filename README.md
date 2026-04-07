
Overview
A production-ready data pipeline implementing Medallion Architecture (Bronze → Silver → Gold) for e-commerce analytics. Built with Apache Airflow, dbt, Trino, and Apache Iceberg

Prerequisites
Docker Desktop 4.20+

8GB+ RAM allocated to Docker

20GB free disk space

Access Services
Service	URL	Credentials
Airflow UI	http://localhost:8080	airflow / airflow
Trino UI	http://localhost:9080	No auth
MinIO Console	http://localhost:9001	minioadmin / miniopassword
Nessie UI	http://localhost:19120	No auth

1. Bronze Layer (Raw Ingestion)
Seeds CSV data into Iceberg tables

Tables: raw_amazon_sales, raw_international_sales, raw_stock_levels

Tag: bronze

2. Silver Layer (Cleansing & Validation)
Data cleaning, deduplication, type casting

Business rule validation

Tag: silver

3. Gold Layer (Aggregation)
Business KPIs and metrics

Aggregated views for BI tools

Tag: gold

4. Documentation
Auto-generates dbt docs

Access at: http://localhost:8080/docs

<img width="2777" height="231" alt="deepseek_mermaid_20260407_77e54e" src="https://github.com/user-attachments/assets/efdacb69-65f1-4165-b00e-bd64cc02f295" />



