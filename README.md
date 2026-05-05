<!-- ================================================================= -->
<!-- FILE: README.md                                                   -->
<!-- ================================================================= -->

# TransJakarta Public Transport Pipeline
### End-to-End Medallion Architecture (PySpark + dbt + Databricks)

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-1E88E5?style=flat)

---

## Overview

This project builds a production-grade data pipeline for TransJakarta — Jakarta's public Bus Rapid Transit (BRT) system. The dataset contains millions of passenger tap-in and tap-out records, capturing journey data across the network.

The pipeline implements a hybrid **Medallion Architecture**. **PySpark** handles the heavy lifting of raw ingestion and structural data quality (Bronze → Silver), while **dbt (data build tool)** takes over to engineer a Kimball-style Star Schema for the analytical Gold layer. The entire ecosystem is hosted on **Databricks** using **Delta Lake**, with **Unity Catalog** governing the assets. 

This architecture guarantees that the final reporting layer is not just clean, but fully tested, documented, and optimized for BI workloads.

---

## Architecture

```text
Raw CSV (Kaggle)
      │
      ▼
┌─────────────┐
│   BRONZE    │  PySpark: Raw ingestion — data landed as-is into Delta
└──────┬──────┘
       │
       ▼
┌─────────────┐  PySpark: Schema enforcement, null handling,
│   SILVER    │  deduplication, quality flagging
└──────┬──────┘  
       │
       ▼
┌─────────────┐  dbt: Dimensional modeling (Star Schema)
│    GOLD     │  fct_transaction + dim_station, dim_date, dim_cards
└──────┬──────┘  Rigorous YAML testing (not_null, unique, relationships)
       │
       ▼
┌─────────────┐
│  ANALYTICS  │  Databricks dashboards answering
└─────────────┘  operational business questions
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks** | Cloud data platform and compute cluster |
| **PySpark** | Distributed data processing (Bronze/Silver layers) |
| **dbt Core** | Data transformation, dimensional modeling, and testing (Gold layer) |
| **Delta Lake** | ACID-compliant storage format |
| **Unity Catalog** | Data governance and three-part naming |

---

## Dataset

**Source**: [TransJakarta Dataset — Kaggle](https://www.kaggle.com/datasets/dikisahkan/transjakarta-transportation-transaction)

**Description**: Passenger tap-in/tap-out records from the TransJakarta BRT network. Each row represents one journey attempt, containing passenger info, corridor details, station origin/destination, timestamps, and payment amount.

---

## Pipeline Layers

### 🥉 Bronze — Raw Ingestion (PySpark)
Lands the raw CSV data into a Delta table with zero transformation. Preserves the source data exactly as received — no filtering, no cleaning, no business logic.
- **Output**: `transjakarta_dataset.bronze.transjakarta_raw`

### 🥈 Silver — Data Quality (PySpark)
Applies structural data quality. Silver makes data trustworthy, it does not make business decisions.
- Schema enforcement and datatype casting.
- Null handling and forward-filling via window functions.
- Deduplication on `transID`.
- Unresolvable records (e.g., missing tap-outs) are flagged, never dropped, preserving full data lineage.
- **Output**: `transjakarta_dataset.silver.transjakarta_cleaned`

### 🥇 Gold — The Star Schema (dbt)
The analytical powerhouse of the pipeline. Built using dbt to transform the flat Silver logs into a scalable dimensional model optimized for BI.

**Fact Table:**
- `fct_transaction`: The core event log. Contains measurable metrics (`payment_amount`) and foreign keys linking to dimensions. Implements role-playing dimensions (mapping both `tap_in_id` and `tap_out_id` to the station dimension).

**Dimension Tables:**
- `dim_station`: Master geography list combining all unique tap-in and tap-out locations via `UNION ALL` logic.
- `dim_cards`: Unique customer transit cards.
- `dim_date`: Calendar dimension parsed from raw timestamps to pre-calculate business logic (e.g., `is_weekend`).

**Data Testing & CI/CD:**
The Gold layer is protected by strict dbt YAML tests:
- `unique` and `not_null` constraints on all Dimension Primary Keys.
- `relationships` (Referential Integrity) tests guaranteeing every Fact row connects to a valid Dimension.
- `accepted_values` checks on demographic data.

---

## Analytics

Dashboards built in Databricks querying the dbt Gold tables directly:

| Dashboard | Key Insight |
|---|---|
| **Daily Revenue Trend** | Revenue performance over the month |
| **Top 10 Busiest Routes** | Which corridors need more bus capacity |
| **Traffic by Day of Week** | Peak days and weekend vs weekday split |
| **Busiest Stations** | Stations requiring operational attention |



---

## Project Structure

```text
transjakarta-pipeline/
├── README.md
├── 01_ingesting_to_raw.ipynb      # PySpark Bronze ingestion
├── 02_raw_to_bronze.ipynb         # PySpark Bronze processing
├── 03_bronze_to_silver.ipynb      # PySpark Silver cleaning
├── dbt_project.yml                # dbt configuration
└── models/
    ├── staging/
    │   ├── src_transjakarta.yml   # Source definitions
    │   └── stg_transjakarta.sql   # Base staging views
    └── marts/
        ├── dim_cards.sql          # Dimension: Cards
        ├── dim_date.sql           # Dimension: Calendar
        ├── dim_station.sql        # Dimension: Stations
        ├── fct_transaction.sql    # Fact: Trips
        └── schema.yml             # dbt Tests & Documentation
```

---

## Author

**Kennys** — Aspiring Data Engineer based in Jakarta, Indonesia.
Transitioning into MLOps by building production-grade, tested, and scalable data architectures.

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/kennys21)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/Kennys1)


<!-- ================================================================= -->
<!-- FILE: models/marts/dim_station.sql                                -->
<!-- ================================================================= -->

```sql
WITH all_stations AS (
    -- The Tap-In Pipe
    SELECT
        tap_in_latitude AS station_latitude,
        tap_in_longitude AS station_longitude,
        tap_in_id AS station_id,
        tap_in_name AS station_name,
        corridor_id,
        corridor_name
    FROM {{ ref('stg_transjakarta') }}

    UNION ALL

    -- The Tap-Out Pipe
    SELECT
        tap_out_latitude AS station_latitude,
        tap_out_longitude AS station_longitude,
        tap_out_id AS station_id,
        tap_out_name AS station_name,
        corridor_id,
        corridor_name
    FROM {{ ref('stg_transjakarta') }}
),

deduplicated_station AS (
    -- The Aggregation Squeeze
    SELECT
        station_id,
        CASE 
            WHEN station_id = '-1' THEN 'Unknown/Glitch Station'
            ELSE MAX(station_name) 
        END AS station_name,
        MAX(station_latitude) AS station_latitude,
        MAX(station_longitude) AS station_longitude,
        MAX(corridor_id) AS corridor_id,
        MAX(corridor_name) AS corridor_name
    FROM all_stations
    WHERE station_id IS NOT NULL
    GROUP BY station_id
)

SELECT * FROM deduplicated_station
```

<!-- ================================================================= -->
<!-- FILE: models/marts/dim_date.sql                                   -->
<!-- ================================================================= -->

```sql
WITH raw_dates AS (
    SELECT CAST(tap_in_time AS DATE) AS date_day 
    FROM {{ ref('stg_transjakarta') }}
    WHERE tap_in_time IS NOT NULL

    UNION 

    SELECT CAST(tap_out_time AS DATE) AS date_day 
    FROM {{ ref('stg_transjakarta') }}
    WHERE tap_out_time IS NOT NULL
)

SELECT
    date_day AS date_key,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    CASE 
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN True 
        ELSE False 
    END AS is_weekend
FROM raw_dates
WHERE date_day IS NOT NULL
ORDER BY date_key
```

<!-- ================================================================= -->
<!-- FILE: models/marts/fct_transaction.sql                            -->
<!-- ================================================================= -->

```sql
WITH final_facts AS (
    SELECT
        transaction_id,
        card_id,
        CAST(tap_in_time AS DATE) AS date_key,
        tap_in_id,
        tap_out_id,
        customer_gender,
        CAST(payment_amount AS INT) AS payment_amount
    FROM {{ ref('stg_transjakarta') }}
    WHERE transaction_id IS NOT NULL
)

SELECT * FROM final_facts
```

<!-- ================================================================= -->
<!-- FILE: models/marts/schema.yml                                     -->
<!-- ================================================================= -->

```yaml
version: 2

models:
  # --- DIMENSION: CARDS ---
  - name: dim_cards
    description: "dimension table for customer cards"
    columns:
      - name: card_id
        description: "the unique card ID for every customer"
        tests:
          - unique:
              config: 
                severity: error
                store_failures: true
          - not_null:
              config:
                severity: error
                store_failures: true
  
  # --- DIMENSION: STATIONS ---
  - name: dim_station
    description: "dimension table for every station"
    columns:
      - name: station_id
        description: "the unique station ID for every station"
        tests:
          - unique:
              config: 
                severity: error
                store_failures: true
          - not_null:
              config:
                severity: error
                store_failures: true
  
  # --- DIMENSION: DATES ---
  - name: dim_date
    description: "dimension table for dates"
    columns:
      - name: date_key
        description: "the unique day for every data"
        tests:
          - unique:
              config: 
                severity: error
                store_failures: true
          - not_null:
              config:
                severity: error
                store_failures: true
  
  # --- FACT: TRANSACTIONS ---
  - name: fct_transaction
    description: "fact table for every trip that happens"
    columns:
      - name: transaction_id
        description: "the transaction ID for every transaction"
        tests:
          - unique:
              config: 
                severity: error
                store_failures: true
          - not_null:
              config:
                severity: error
                store_failures: true
                
      - name: customer_gender
        description: "the gender for every customer"
        tests:
          - accepted_values:
              arguments:
                values: ['M','F']
              config: 
                where: "customer_gender IS NOT NULL"
                severity: warn
                store_failures: true
                
      - name: tap_in_id
        tests:
          - not_null       
          - relationships: 
              arguments:
                to: ref('dim_station')
                field: station_id
                
      - name: tap_out_id
        tests:
          - not_null       
          - relationships: 
              arguments:
                to: ref('dim_station')
                field: station_id
                
      - name: date_key
        tests:
          - not_null       
          - relationships: 
              arguments:
                to: ref('dim_date')
                field: date_key
                
      - name: payment_amount
        tests:
          - not_null:
              config:
                severity: error
                store_failures: true
```
