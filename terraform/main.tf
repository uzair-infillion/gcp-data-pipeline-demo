terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Bucket (DAGs + staging) ──────────────────────────────────────────────
resource "google_storage_bucket" "dags_bucket" {
  name                        = "${var.project_id}-composer-dags"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }

  labels = var.labels
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "pmp_reporting" {
  dataset_id                 = "pmp_reporting"
  friendly_name              = "PMP Unified Reporting"
  description                = "Unified Private Marketplace reporting data from all SSP partners"
  location                   = var.bq_location
  delete_contents_on_destroy = false
  labels                     = var.labels
}

# ── BigQuery Table ────────────────────────────────────────────────────────────
resource "google_bigquery_table" "unified_pmp_report" {
  dataset_id          = google_bigquery_dataset.pmp_reporting.dataset_id
  table_id            = "unified_pmp_report"
  deletion_protection = true
  description         = "Unified daily PMP report — all SSP partners in one schema"
  labels              = var.labels

  time_partitioning {
    type  = "DAY"
    field = "report_date"
  }

  clustering = ["partner", "deal_id"]

  schema = file("${path.module}/../schemas/pmp_report.json")
}

# ── Cloud Composer Environment ────────────────────────────────────────────────
resource "google_composer_environment" "pmp_pipeline" {
  name   = "pmp-pipeline-${var.environment}"
  region = var.region
  labels = var.labels

  config {
    software_config {
      image_version = "composer-2-airflow-2"
      pypi_packages = {
        "apache-airflow-providers-google" = ">=10.0.0"
        "requests"                        = ">=2.28.0"
      }
      env_variables = {
        GCP_PROJECT_ID = var.project_id
        BQ_DATASET     = google_bigquery_dataset.pmp_reporting.dataset_id
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu       = 0.5
        memory_gb = 1.875
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 1.875
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }
}

# ── IAM: Composer SA → BigQuery ───────────────────────────────────────────────
resource "google_project_iam_member" "composer_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_composer_environment.pmp_pipeline.config[0].node_config[0].service_account}"
}

resource "google_project_iam_member" "composer_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_composer_environment.pmp_pipeline.config[0].node_config[0].service_account}"
}

resource "google_project_iam_member" "composer_gcs_editor" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_composer_environment.pmp_pipeline.config[0].node_config[0].service_account}"
}
