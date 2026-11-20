
# Multi-Source Analytics Lakehouse

A production-oriented, open-source data engineering project that builds a Medallion Lakehouse architecture to ingest, process, transform, and analyze data from multiple sources using Apache Spark, Apache Iceberg, Project Nessie, Trino, and Apache Airflow.

The project simulates a retail analytics platform called **RetailPulse Analytics**, integrating customer, transaction, payment, and financial data into a centralized analytics environment.

---

## 1. Project Overview

Modern businesses generate data across multiple systems, including REST APIs, relational databases, and external files.

This project demonstrates how to build a centralized Lakehouse platform that:

- Ingests data from multiple heterogeneous sources.
- Stores raw data in S3-compatible object storage.
- Processes and cleans data using PySpark.
- Manages analytical tables using Apache Iceberg.
- Uses Project Nessie for data catalog and metadata management.
- Builds business-ready Gold datasets.
- Enables SQL analytics through Trino.
- Automates pipeline execution using Apache Airflow.
- Provisions infrastructure using Terraform.
- Validates pipeline components using Pytest.
- Runs automated tests through GitHub Actions.

## 2. Architecture

### High-Level Architecture

```text
              DATA SOURCES
                   |
       +-----------+-----------+
       |           |           |
     REST API   PostgreSQL   CSV / JSON
       |           |           |
       +-----------+-----------+
                   |
             INGESTION LAYER
                   |
          Apache Airflow DAG
                   |
                   v
          BRONZE DATA LAYER
           Raw JSON / CSV
                   |
          S3-Compatible Storage
                   |
                   v
          SILVER DATA LAYER
       Cleaned and Deduplicated
          Apache Spark + Iceberg
                   |
                   v
           GOLD DATA LAYER
       Business-Ready Analytics
          Apache Spark + Iceberg
                   |
                   v
             QUERY LAYER
                  Trino
                   |
                   v
          Business Analytics
```

### Medallion Architecture

| Layer | Purpose |
|---|---|
| Bronze | Stores raw source data with ingestion metadata. |
| Silver | Cleans, validates, deduplicates, and standardizes data. |
| Gold | Creates business-level analytical datasets and aggregates. |

## 3. Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python |
| Data Processing | Apache Spark, PySpark |
| Data Lakehouse | Apache Iceberg |
| Object Storage | SILO (S3-compatible storage) |
| Metadata Catalog | Project Nessie |
| SQL Query Engine | Trino |
| Source Database | PostgreSQL |
| Workflow Orchestration | Apache Airflow |
| Infrastructure as Code | Terraform |
| Containerization | Docker, Docker Compose |
| Testing | Pytest, pytest-cov |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |

## 4. Project Structure

```text
multi-source-analytics-lakehouse/
│
├── config/
│   ├── spark.py
│   ├── iceberg_spark.py
│   └── storage.py
│
├── ingestion/
│   ├── api/
│   │   └── ingest_customers.py
│   ├── postgres/
│   │   └── ingest_postgres.py
│   └── files/
│       └── ingest_files.py
│
├── processing/
│   ├── bronze/
│   ├── silver/
│   │   ├── iceberg_writer.py
│   │   └── run_silver.py
│   └── gold/
│       └── run_gold.py
│
├── quality/
│   ├── data_quality.py
│   └── run_quality_checks.py
│
├── orchestration/
│   └── dags/
│       └── retailpulse_lakehouse_dag.py
│
├── infrastructure/
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   ├── .env
│   │   └── trino/
│   │       └── catalog/
│   └── terraform/
│       ├── main.tf
│       ├── providers.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── README.md
│
├── sql/
├── tests/
├── docs/
├── logs/
│
├── .github/
│   └── workflows/
│       └── tests.yml
│
├── requirements.txt
├── .gitignore
└── README.md
```

## 5. Data Sources

The platform integrates three different types of data sources.

### REST API

Source: DummyJSON Users API

Endpoint:

https://dummyjson.com/users

Implementation:
- Retrieves customer records using API pagination.
- Extracts customer information.
- Adds ingestion metadata.
- Stores raw API responses in Bronze.
- Uses ingestion date and run ID for partitioned object paths.

### PostgreSQL

Source database: RetailPulse

Tables:
- customers
- orders
- order_items

Implementation:
- Connects to PostgreSQL using Python.
- Extracts relational records.
- Converts the extracted data into JSON.
- Uploads the raw data to Bronze storage.

### CSV and JSON Files

Financial source files:
- payments.csv
- refunds.json

Implementation:
- Reads CSV and JSON files.
- Preserves the original source files.
- Uploads them into Bronze storage.
- Uses unique ingestion run IDs.

## 6. Bronze Layer – Raw Data Ingestion

The Bronze layer stores the original data extracted from source systems.

Key features:
- Raw data preservation.
- Multiple source integration.
- Ingestion date partitioning.
- Unique run IDs.
- Source metadata.
- S3-compatible object storage.

Example object paths:

```text
customers/
  ingestion_date=2026-11-20/
    run_id=<unique-run-id>/
      customers.json
```

The Bronze layer allows downstream processing without directly depending on source systems.

## 7. Silver Layer – Data Cleaning and Transformation

The Silver layer uses PySpark to clean and standardize Bronze data.

Transformations include:
- Removing duplicate customer records.
- Standardizing customer information.
- Cleaning PostgreSQL customer records.
- Processing orders and order items.
- Cleaning payment information.
- Processing refund records.
- Applying data quality validations.

### Silver Iceberg Tables

| Table | Description |
|---|---|
| api_customers | Cleaned API customer records |
| postgres_customers | Cleaned PostgreSQL customers |
| orders | Cleaned order transactions |
| order_items | Cleaned order item records |
| payments | Cleaned payment records |
| refunds | Cleaned refund records |

Apache Iceberg provides table management and metadata capabilities for the Silver datasets.

Project Nessie manages the Iceberg catalog.

## 8. Gold Layer – Business Analytics

The Gold layer contains business-ready analytical datasets.

Four analytical tables are created.

### Monthly Revenue

Table: `monthly_revenue`

Metrics:
- Total revenue.
- Total orders.
- Average order value.
- Completed orders.
- Cancelled orders.

### Customer Purchase Summary

Table: `customer_purchase_summary`

Metrics:
- Total orders per customer.
- Total spending.
- Average order value.
- Last order date.

### Payment Summary

Table: `payment_summary`

Metrics:
- Payment method.
- Payment status.
- Total payment count.
- Total payment amount.
- Average payment amount.

### Product Sales Summary

Table: `product_sales_summary`

Metrics:
- Product sales revenue.
- Total units sold.
- Item count.
- Average selling price.

These datasets provide a foundation for business intelligence and analytical reporting.

## 9. SQL Analytics with Trino

Trino is configured to query Apache Iceberg tables through Project Nessie.

Capabilities:
- SQL-based analytical queries.
- Access to Silver and Gold datasets.
- Aggregations and joins.
- Analytical exploration without directly reading raw files.

Example:

```sql
SHOW CATALOGS;

SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.gold;

SELECT *
FROM iceberg.gold.monthly_revenue;
```

The exact catalog name used in Trino is `iceberg`.

## 10. Workflow Orchestration with Apache Airflow

Apache Airflow automates the end-to-end pipeline.

DAG:

`retailpulse_lakehouse_pipeline`

Workflow:

```text
          START
            |
     +------+------+ 
     |      |      |
    API  PostgreSQL Files
     |      |      |
     +------+------+ 
            |
      SILVER PROCESSING
            |
      GOLD PROCESSING
            |
           END
```

The three ingestion tasks execute independently before Silver processing begins.

Gold processing starts only after Silver completes successfully.

Features:
- DAG-based orchestration.
- Task dependencies.
- Automatic retries.
- Scheduled execution.
- Pipeline monitoring through the Airflow UI.

Airflow UI:

```text
http://localhost:8083
```

The configured schedule is `0 6 * * *` UTC, equivalent to 11:30 AM IST.

## 11. Infrastructure as Code with Terraform

Terraform manages a dedicated Docker network for the project.

Resources:
- Docker network.
- Network driver configuration.
- Internal network configuration.
- Terraform state management.
- Output values for resource identification.

Commands:

```bash
cd infrastructure/terraform

terraform init

terraform fmt

terraform validate

terraform plan

terraform apply
```

To inspect the managed infrastructure:

```bash
terraform state list

terraform output

terraform plan
```

The existing Docker Compose infrastructure is kept separate from Terraform ownership to avoid conflicting resource management.

## 12. Automated Testing

The project uses Pytest to validate ingestion, storage, Bronze extraction, Silver transformations, and Gold analytics.

### Run All Tests

```bash
python -m pytest tests/ -v
```

### Run Individual Test Modules

```bash
python -m pytest tests/test_storage_connection.py -v

python -m pytest tests/test_file_ingestion.py -v

python -m pytest tests/test_bronze_extraction.py -v

python -m pytest tests/test_bronze_readers.py -v

python -m pytest tests/test_silver_customers.py -v

python -m pytest tests/test_silver_finance.py -v

python -m pytest tests/test_silver_postgres_customers.py -v

python -m pytest tests/test_silver_transactions.py -v

python -m pytest tests/test_monthly_revenue.py -v

python -m pytest tests/test_customer_purchase_summary.py -v

python -m pytest tests/test_payment_summary.py -v

python -m pytest tests/test_product_sales_summary.py -v
```

### Test Coverage

Install coverage support:

```bash
python -m pip install pytest-cov
```

Generate a terminal coverage report:

```bash
python -m pytest tests/ \
  --cov=ingestion \
  --cov=processing \
  --cov=quality \
  --cov=config \
  --cov-report=term-missing
```

Generate an HTML report:

```bash
python -m pytest tests/ \
  --cov=ingestion \
  --cov=processing \
  --cov=quality \
  --cov=config \
  --cov-report=html
```

Open the report:

```bash
explorer.exe htmlcov/index.html
```

### Current Test Status

The existing test suite contains 15 tests.

Latest recorded test execution:

```text
15 passed
```

## 13. Continuous Integration with GitHub Actions

The project includes a GitHub Actions workflow:

```text
.github/workflows/tests.yml
```

The workflow is configured to run when:
- Code is pushed to the main branch.
- A pull request targets the main branch.

The workflow:
1. Checks out the repository.
2. Sets up Python.
3. Installs Java.
4. Installs project dependencies.
5. Executes the automated test suite.

Note: Tests requiring live infrastructure must have their required services and credentials configured in the CI environment before the workflow can pass.

## 14. Local Setup

### Prerequisites

Install:
- Docker Desktop with WSL2 integration.
- Python 3.11 recommended for the PySpark 3.5.6 environment.
- Java 17.
- Git.
- Terraform.

### Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>

cd multi-source-analytics-lakehouse
```

### Create a Virtual Environment

```bash
python3.11 -m venv .venv

source .venv/bin/activate
```

### Install Dependencies

```bash
python -m pip install --upgrade pip

pip install -r requirements.txt
```

### Configure Environment Variables

Create the Docker environment configuration:

```bash
cp infrastructure/docker/.env.example infrastructure/docker/.env
```

If the example file is not present, create `infrastructure/docker/.env` using the environment variable names documented in the project configuration.

Configure the required PostgreSQL and S3-compatible storage credentials.

Never commit `.env` files or real credentials to GitHub.

### Start Docker Infrastructure

```bash
cd infrastructure/docker

docker compose up -d
```

Verify containers:

```bash
docker compose ps
```

Start the pipeline components after confirming that their dependencies are healthy.

## 15. Useful Service URLs

| Service | Local URL |
|---|---|
| S3-compatible storage API | http://localhost:9000 |
| Storage Console | http://localhost:9001 |
| Trino | http://localhost:8080 |
| Project Nessie | http://localhost:19120 |
| Apache Airflow | http://localhost:8083 |
| PostgreSQL | localhost:5432 |

## 16. Future Improvements

Potential extensions include:

- Incremental ingestion using watermarks.
- Deterministic deduplication using event timestamps.
- Slowly Changing Dimensions (SCD Type 2).
- Data quarantine and rejected-record handling.
- Advanced schema evolution.
- Incremental Iceberg MERGE operations.
- Streaming ingestion using Apache Kafka.
- Data lineage and observability.
- Prometheus and Grafana monitoring.
- Cloud deployment on AWS.
- Improved CI integration tests with service containers.
- Data quality metrics and automated alerts.

## 17. Key Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- Multi-source data ingestion.
- Batch data engineering pipelines.
- Medallion Lakehouse architecture.
- Object storage and data lakes.
- Apache Iceberg table formats.
- Data catalogs and metadata management.
- Distributed data processing with PySpark.
- Data cleaning and transformation.
- SQL analytics with Trino.
- Workflow orchestration with Airflow.
- Infrastructure as Code using Terraform.
- Docker-based infrastructure.
- Automated testing and code coverage.
- Continuous Integration using GitHub Actions.

## 18. Project Goal

The goal of this project is to demonstrate how a modern data engineering platform can integrate multiple source systems, transform raw data into reliable analytical datasets, and provide a scalable foundation for business intelligence and future cloud-based data engineering workloads.
