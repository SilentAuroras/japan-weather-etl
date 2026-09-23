// Artifact Registry for Docker Images
resource "google_artifact_registry_repository" "weather_etl_repo" {
  repository_id = "weather-etl-repo"
  description   = "Docker Repository for Weather ETL"
  format        = "DOCKER"
  project       = var.project
  location      = var.region
}