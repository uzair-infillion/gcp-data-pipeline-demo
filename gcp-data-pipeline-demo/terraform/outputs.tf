output "composer_gcs_bucket" {
  description = "GCS bucket used by Cloud Composer for DAGs"
  value       = google_composer_environment.pmp_pipeline.config[0].dag_gcs_prefix
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL"
  value       = google_composer_environment.pmp_pipeline.config[0].airflow_uri
}

output "bq_table_id" {
  description = "Full BigQuery table ID"
  value       = "${var.project_id}.${google_bigquery_dataset.pmp_reporting.dataset_id}.${google_bigquery_table.unified_pmp_report.table_id}"
}

output "dags_bucket_name" {
  description = "GCS bucket name for DAG storage"
  value       = google_storage_bucket.dags_bucket.name
}
