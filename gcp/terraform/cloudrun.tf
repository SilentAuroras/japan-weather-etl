// Cloud Run Function
resource "google_cloud_run_v2_service" "default" {
  name                = local.cloud_run_service_name
  location            = var.region
  deletion_protection = false

  // Wait for artifact registry
  depends_on = [
    google_artifact_registry_repository.weather_etl_repo,
    terraform_data.build_image
  ]

  // Configure service
  template {

    // Configure execution permissions
    execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
    service_account       = google_service_account.sa-weather.email

    // Disable health check
    health_check_disabled = true

    // Docker image from artifact registry
    containers {

      // Define image name from artifact registry
      image = local.container_image

      // Volume to get output, mount docker image to GCS bucket
      volume_mounts {
        mount_path = "/app/data/"
        name       = "bucket"
      }

      # Env variables for overpass headers - user_agent
      env {
        name  = "OVERPASS_USER_AGENT"
        value = var.overpass_user_agent
      }

      # Env variables for overpass headers - referer
      env {
        name  = "OVERPASS_REFERER"
        value = var.overpass_referer
      }

      # Env variable to load GSC parquet files into BigQuery
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.japan-weather-docker-mount.name
      }

      # Env variable for BigQuery dataset load
      env {
        name  = "BIGQUERY_DATASET"
        value = "japan_weather_dataset_001"
      }
    }

    // Set storage to gcs
    volumes {
      name = "bucket"
      gcs {
        bucket    = google_storage_bucket.japan-weather-docker-mount.name
        read_only = false
      }
    }
  }
}

// Scheduler Cron Job
resource "google_cloud_scheduler_job" "scheduler-job" {
  name        = "japan-weather-etl-cron"
  description = "Trigger to run weather docker image"

  // 2am daily UTC
  schedule  = "0 2 * * *"
  time_zone = "Etc/UTC"

  // Wait for SA account
  depends_on = [
    google_service_account.sa-weather,
    google_project_iam_member.sa-weather-invoker,
    google_project_iam_member.sa-weather-bigquery-editor,
    google_project_iam_member.sa-weather-bigquery-jobs
  ]

  # Request target to cloudrun url
  http_target {

    // Define request
    uri         = google_cloud_run_v2_service.default.urls[0]
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }

    // Use scheduler account
    oidc_token {
      service_account_email = google_service_account.sa-weather.email
      audience              = google_cloud_run_v2_service.default.urls[0]
    }
  }
}