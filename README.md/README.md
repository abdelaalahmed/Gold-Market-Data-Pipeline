# 🪙 Gold Market & Inflation Analytics Platform (2013 - 2026)

An End-to-End Data Engineering Pipeline built on Azure and Databricks to ingest, process, and analyze historical and real-time gold market pricing against inflation indicators.

## 🛠️ Tech Stack & Architecture
- **Cloud Infrastructure:** Azure
- **Data Lakehouse:** Azure Databricks (Delta Lake)
- **Data Processing:** PySpark / Spark SQL
- **Data Modeling:** Star Schema (Dimensional Modeling)
- **BI & Visualization:** Power BI (Import Mode)

## 🏗️ Data Pipeline Stages (Medallion Architecture)
1. **Bronze Layer:** Ingested raw JSON/CSV data from APIs using Python and appended them safely into Delta Tables.
2. **Silver Layer:** Cleaned data with PySpark, casted data types, handled missing values, and calculated the `Daily_Spread` (High - Low) as a volatility index.
3. **Gold Layer:** Modeled data into a Star Schema consisting of 1 Fact table (`fact_gold_prices`) and 3 Dimension tables (`dim_gold_date`, `dim_gold_inflation`, `dim_gold_market_status`).

## 📊 Business Intelligence & Optimization
- Connected Power BI to Databricks SQL Warehouse using **Import Mode** to optimize performance and prevent runtime cloud costs.
- Designed an interactive dashboard with time-based slicers (2013-2026) for trend analysis.