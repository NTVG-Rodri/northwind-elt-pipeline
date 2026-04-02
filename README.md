# Northwind dbt Pipeline

An end-to-end data engineering pipeline built on the classic Northwind database. The project covers the full analytics engineering lifecycle: raw ingestion into PostgreSQL, layered transformation with dbt, orchestration with Airflow, and business dashboards with Metabase — fully containerized with Docker Compose.



## Architecture

```
PostgreSQL (raw Northwind data)
          │
          ▼
    dbt staging layer        ← type casting, column renaming, source cleaning
          │
          ▼
     dbt marts layer         ← business models using window functions and CTEs
          │
          ▼
        Metabase              ← dashboards connected to mart tables
          
    Apache Airflow            ← orchestrates and schedules the full pipeline
```


## Tech Stack

_ Database: PostgreSQL 13 
_ Transformation: dbt-postgres 1.9 
_ Orchestration: Apache Airflow 2.8 
_ Visualization: Metabase latest 
_ Infrastructure: Docker & Docker Compose 
_ Dev environment: WSL2 + VSCode 


## Data Model

### Staging (`models/staging/`)

Each staging model maps 1:1 to a Northwind source table. Responsibilities are limited to column renaming, type casting, and light cleaning. No business logic at this layer.


`stg_customers`: customers 
`stg_orders`: orders 
`stg_order_details`: order_details 
`stg_products`: products 
`stg_employees`: employees 
`stg_categories`: categories 

### Marts (`models/marts/`)

Business-ready models built on top of staging. Each model answers a specific analytical question.


`customer_segmentation`: Classifies customers as HIGH / MEDIUM / LOW by revenue 
`customer_ranking`: Customers ranked by total revenue 
`avg_order_value`: Average, minimum and maximum ticket per customer
`repeat_customers`: Customers with more than one order, with first and last purchase dates 
`employee_performance`: Revenue and total orders per employee 
`sales_by_category`: Revenue broken down by product category 
`sales_by_country`: Orders and freight costs grouped by shipping country 
`sales_by_month`: Monthly revenue with cumulative total 
`top_products`: Products ranked by total revenue 



## Data Quality

All staging and mart models have dbt schema tests defined in `schema.yml`:

- `not_null` on primary keys and key business columns
- `unique` on primary keys
- `accepted_values` on categorical columns (e.g. segment values: `HIGH`, `MEDIUM`, `LOW`)

Run all tests:

```bash
dbt test
```

Run tests for a single layer:

```bash
dbt test --select staging
dbt test --select marts
```


## Orchestration

The Airflow DAG runs the full pipeline in sequence:

1. `load_raw_data` — Python operator that loads Northwind source data into PostgreSQL
2. `dbt_build` — Bash operator that runs `dbt build`, executing all models and tests in dependency order



## Getting Started

**Prerequisites:** Docker and Docker Compose installed.

```bash
# 1. Clone the repository
git clone https://github.com/NTVG-Rodri/northwind-dbt-pipeline.git
cd northwind-dbt-pipeline

# 2. Set up environment variables
cp .env.example .env

# 3. Start all services
docker-compose up -d
```


## Project Structure

```
northwind-dbt-pipeline/
├── dags/
│   └── dag_northwind_pipeline.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   └── stg_*.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── customer_segmentation.sql
│   │       ├── customer_ranking.sql
│   │       ├── avg_order_value.sql
│   │       ├── repeat_customers.sql
│   │       ├── employee_performance.sql
│   │       ├── sales_by_category.sql
│   │       ├── sales_by_country.sql
│   │       ├── sales_by_month.sql
│   │       └── top_products.sql
│   ├── dbt_project.yml
│   └── profiles.yml.example
├── docs/
│   └── lineage_graph.png
├── docker-compose.yaml
└── README.md
```


## Key SQL patterns used

- `NTILE()` for dynamic customer segmentation
- `RANK()` and `ROW_NUMBER()` for product, customer, and employee rankings
- `SUM() OVER` for cumulative revenue by month
- CTEs for multi-step transformations
- Multi-table JOINs across staging models via `ref()`
- `MIN()` / `MAX()` for customer purchase history
