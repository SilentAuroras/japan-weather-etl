// GCS bucket for docker image output
resource "google_storage_bucket" "docker-mount-gcs" {
  name     = "docker-mount-gcs"
  location = var.region
  force_destroy = true
  public_access_prevention = "enforced"
}

// Service account to write to gcs
resource "google_service_account" "run_sa" {
  account_id   = "run-sa"
  display_name = "Cloud Run runtime service account"
}

// GCS storage account bucket
resource "google_storage_bucket_iam_member" "run_sa_writer" {
  bucket = google_storage_bucket.docker-mount-gcs.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.run_sa.email}"
}

// Cloud Run Function
resource "google_cloud_run_v2_service" "default" {
  name     = "japan-weather-etl-docker"
  location = var.region
  deletion_protection = false

  template {
    execution_environment = "EXECUTION_ENVIRONMENT_GEN2"
    service_account = google_service_account.run_sa.email

    // Disable health check
    health_check_disabled = true

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project}/weather-etl-repo/japan-weather-etl:tag1"
      
      // Volume to get output, mount docker image to GCS bucket
      volume_mounts {
        mount_path = "/app/data/"
        name       = "bucket"
      }
    }
    volumes {
      name = "bucket"
      gcs {
        bucket = google_storage_bucket.docker-mount-gcs.name
        read_only = false
      }
    }
  }
}

// Scheduler service account
resource "google_service_account" "scheduler_sa" {
  account_id = "scheduler-sa"
  display_name = "Cloud Scheduler Service Account"
}

// Scheduler Membership
resource "google_project_iam_member" "scheduler_sa_membership" {
  member  = "serviceAccount:${google_service_account.scheduler_sa.email}"
  project = var.project
  role    = "roles/run.invoker"
}

// Scheduler Cron Job
resource "google_cloud_scheduler_job" "scheduler-job" {
  name = "japan-weather-etl-cron"
  description = "Trigger to run weather docker image"
  
  // 2am daily UTC
  schedule = "0 2 * * *"
  time_zone = "Etc/UTC"
  
  http_target {
    uri = google_cloud_run_v2_service.default.urls[0]
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
      audience = google_cloud_run_v2_service.default.urls[0]
    }
  }
}