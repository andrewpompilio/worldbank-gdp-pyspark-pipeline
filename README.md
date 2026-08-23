# World Bank GDP PySpark Data Pipeline (Medallion Architecture)

An end-to-end data engineering pipeline built in **Databricks** using **PySpark** and **Delta Lake**. This project ingests raw global GDP indicators from the World Bank REST API, transforms the data through a Medallion Architecture (Bronze -> Silver -> Gold), and models it into a BI-ready Kimball Star Schema featuring Slowly Changing Dimensions (SCD Type 1 & 2).

---

## 🏗️ Architecture Overview

The pipeline follows the **Medallion Architecture** pattern:

* **Bronze Layer (`01_Bronze_Ingestion.py`)**: Ingests raw JSON data directly from the World Bank REST API via `urllib` and persists it as an append-only Delta table.
* **Silver Layer (`02_Silver_Transformation.py`)**: Parses nested JSON structuress, handles missing values, cleans attribute types, and standardizes metric schemas.
* **Gold Layer (`03_Gold_Load.py`)**: Dimensional modeling phase constructing a Kimball Star Schema optimized for Tableau/Power BI reporting:
  * **`fact_gdp`**: Central fact table storing numeric GDP metrics and dimension surrogate keys.
  * **`dim_date`**: Time lookup dimension enriched with decade and recency flags.
  * **`dim_country`**: Entity dimension implementing **SCD Type 1 & Type 2** via Delta Lake `MERGE` operations.

---

## 🛠️ Key Technical Features

* **SCD Type 1 & Type 2 Handling**: Combines in-place overwrites for minor attribute corrections (ISO code fixes) with historical tracking (`effective_date`, `end_date`, `is_current`) for major entity changes (country renames).
* **Data Quality Assertions**: Automated pre- and post-execution checks enforcing non-null surrogate keys, non-negative metrics, and relational integrity across joins.
* **Delta Lake Transactions**: Leverages ACID transactions and `.merge()` logic to prevent duplicate row creation across incremental runs.

---

## 📊 Star Schema Design
```mermaid
erDiagram
    gold_dim_date ||--o{ gold_fact_gdp : "1:N"
    gold_dim_country ||--o{ gold_fact_gdp : "1:N"

    gold_fact_gdp {
        bigint country_key FK
        int year FK
        string indicator_id
        string indicator_name
        double gdp_usd
    }

    gold_dim_country {
        bigint country_key PK
        string country_id
        string country_name
        string countryiso3code
        date effective_date
        date end_date
        boolean is_current
    }

    gold_dim_date {
        int year PK
        int decade
        boolean is_recent
    }
