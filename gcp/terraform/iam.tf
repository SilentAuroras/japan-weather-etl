// Service Account - Cloud Run and Scheduler
resource "google_service_account" "sa-weather" {
  account_id   = "sa-weather"
  display_name = "Cloud Run runtime service account"
}

// Membership - Add to run cloudrun function invoke rights
resource "google_project_iam_member" "sa-weather-invoker" {
  member  = "serviceAccount:${google_service_account.sa-weather.email}"
  role    = "roles/run.invoker"
  project = var.project
}

// Membership - GCS storage account bucket permissions
resource "google_storage_bucket_iam_member" "sa-weather-gcs" {
  member = "serviceAccount:${google_service_account.sa-weather.email}"
  role   = "roles/storage.objectAdmin"
  bucket = google_storage_bucket.japan-weather-docker-mount.name
}

// Membership - BigQuery load permissions for cloud run service account
resource "google_project_iam_member" "sa-weather-bigquery-editor" {
  member  = "serviceAccount:${google_service_account.sa-weather.email}"
  role    = "roles/bigquery.dataEditor"
  project = var.project
}

// Membership - BigQuery load permissions for loading parquet files into BigQuery tables
resource "google_project_iam_member" "sa-weather-bigquery-jobs" {
  member  = "serviceAccount:${google_service_account.sa-weather.email}"
  role    = "roles/bigquery.jobUser"
  project = var.project
}