# GCP Data Pipeline — PMP Unified Reporting

A production-grade data engineering pipeline built on Google Cloud Platform that ingests Private Marketplace (PMP) bid/impression/revenue reports from multiple Supply-Side Platform (SSP) partners into BigQuery, exposing a single unified reporting schema.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Cloud Composer (Airflow)                     │
│                                                                   │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│  │ Magnite  │   │  OpenX   │   │  Index   │   │  Nexxen  │...  │
│  │  DAG     │   │  DAG     │   │ Exchange │   │  DAG     │    │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘    │
│       │              │              │              │            │
│       └──────────────┴──────────────┴──────────────┘            │
│                              │                                   │
│                    ┌─────────▼──────────┐                       │
│                    │  Schema Validator   │                       │
│                    │  + Normalizer       │                       │
│                    └─────────┬──────────┘                       │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │      BigQuery        │
                    │  pmp_reporting.      │
                    │  unified_pmp_report  │
                    │  (partitioned by     │
                    │   report_date,       │
                    │   clustered by       │
                    │   partner, deal_id)  │
                    └─────────────────────┘
```

## Key Features

- **Unified schema** — 5 SSP partners (Magnite, OpenX, Index Exchange, Nexxen, Catalina) with different raw formats normalized into one consistent BigQuery table
- **Strict schema enforcement** — type validation and field alias resolution per partner; >5% error rate fails the DAG
- **Idempotent loads** — BigQuery partition decorators + `WRITE_TRUNCATE` make every run safely re-runnable
- **Full backfill support** — any historical date range can be re-ingested cleanly
- **Infrastructure as Code** — all GCP resources (Composer, BigQuery, GCS, IAM) managed via Terraform
- **Modern Airflow** — TaskFlow API (@task decorators), no legacy operators

## Project Structure

```
gcp-data-pipeline-demo/
├── dags/
│   ├── pmp_reporting_dag.py      # Main Airflow DAG (TaskFlow API)
│   └── utils/
│       ├── schema_validator.py   # Per-partner validation & normalization
│       └── bigquery_loader.py    # BQ load with partition + error handling
├── schemas/
│   └── pmp_report.json           # BigQuery table schema (unified)
├── terraform/
│   ├── main.tf                   # Composer, BigQuery, GCS, IAM resources
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   └── test_schema_validator.py  # Unit tests (pytest)
└── requirements.txt
```

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.x (Cloud Composer) |
| Data Warehouse | Google BigQuery (partitioned + clustered) |
| Storage | Google Cloud Storage |
| Infrastructure | Terraform |
| Language | Python 3.11 |
| Testing | pytest |

## Getting Started

### Prerequisites

- GCP project with Billing enabled
- `gcloud` CLI authenticated
- Terraform >= 1.3
- Python 3.11+

### 1. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform plan -var="project_id=your-gcp-project" -var="environment=dev"
terraform apply -var="project_id=your-gcp-project" -var="environment=dev"
```

### 2. Upload DAGs to Composer

```bash
# Get the DAGs bucket from Terraform output
DAGS_BUCKET=$(terraform output -raw composer_gcs_bucket)

# Upload DAGs
gsutil -m cp -r ../dags/* gs://${DAGS_BUCKET}/dags/
gsutil cp ../schemas/pmp_report.json gs://${DAGS_BUCKET}/dags/schemas/
```

### 3. Configure Airflow Variables

In the Airflow UI (or via CLI):

```bash
airflow variables set GCP_PROJECT_ID "your-gcp-project"
airflow variables set BQ_DATASET "pmp_reporting"

# Per-partner API keys (in production)
airflow variables set MAGNITE_API_KEY "..."
airflow variables set OPENX_API_KEY "..."
```

### 4. Run Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Expected output:
```
tests/test_schema_validator.py::TestValidateSchema::test_valid_row_passes PASSED
tests/test_schema_validator.py::TestValidateSchema::test_field_alias_magnite PASSED
tests/test_schema_validator.py::TestValidateSchema::test_field_alias_openx PASSED
tests/test_schema_validator.py::TestValidateSchema::test_type_coercion_string_to_int PASSED
...
10 passed in 0.12s
```

## DAG Overview

The `pmp_unified_reporting` DAG runs daily at 06:00 UTC and processes D-1 data:

```
get_report_date
      │
      ├─── fetch_partner_data(magnite) ──► validate_and_normalize(magnite) ──► load_to_bq(magnite)
      ├─── fetch_partner_data(openx)   ──► validate_and_normalize(openx)   ──► load_to_bq(openx)
      ├─── fetch_partner_data(ix)      ──► validate_and_normalize(ix)      ──► load_to_bq(ix)
      ├─── fetch_partner_data(nexxen)  ──► validate_and_normalize(nexxen)  ──► load_to_bq(nexxen)
      └─── fetch_partner_data(catalina)──► validate_and_normalize(catalina)──► load_to_bq(catalina)
                                                                                        │
                                                                                  summarize()
```

## BigQuery Schema

| Column | Type | Description |
|---|---|---|
| `report_date` | DATE | Report date (partition key) |
| `partner` | STRING | SSP partner name |
| `deal_id` | STRING | Unique PMP deal ID |
| `deal_name` | STRING | Deal display name |
| `impressions` | INTEGER | Total impressions served |
| `bids` | INTEGER | Total bid requests |
| `wins` | INTEGER | Total bid wins |
| `revenue_usd` | FLOAT | Gross revenue in USD |
| `cpm` | FLOAT | Effective CPM |
| `fill_rate` | FLOAT | Fill rate (0.0–1.0) |
| `ingested_at` | TIMESTAMP | Ingestion timestamp |

Table is **partitioned by `report_date`** and **clustered by `partner`, `deal_id`** for cost-efficient queries.

## Author

**Uzair Ahmad** — Senior Data Engineer  
[github.com/uzair-infillion](https://github.com/uzair-infillion)
