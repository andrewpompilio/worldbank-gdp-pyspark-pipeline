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
            +-------------------+
            |   gold.dim_date   |
            +-------------------+
            | year (PK)         |
            | decade            |
            | is_recent         |
            +---------+---------+
                      |
                      | 1:N
                      v
+---------------------+---------------------+
|                  gold.fact_gdp            |
+-------------------------------------------+
| country_key (FK)                          |
| year (FK)                                 |
| indicator_id                              |
| indicator_name                            |
| gdp_usd                                   |
+---------------------+---------------------+
                      ^
                      | N:1
                      |
          +-----------+-----------+
          |     gold.dim_country  |
          +-----------------------+
          | country_key (PK)      |
          | country_id            |
          | country_name          |
          | countryiso3code       |
          | effective_date        |
          | end_date              |
          | is_current            |
          +-----------------------+
