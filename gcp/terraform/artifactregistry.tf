// Artifact Registry for Docker Images
resource "google_artifact_registry_repository" "weather_etl_repo" {
  repository_id = "weather-etl-repo"
  description   = "Docker Repository for Weather ETL"
  format        = "DOCKER"
  project       = var.project
  location      = var.region
}

// Hard define docker name
locals {
  cloud_run_service_name = "japan-weather-etl-docker"
  container_image        = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.weather_etl_repo.repository_id}/${local.cloud_run_service_name}:tag1"
  docker_context_files   = concat(["Dockerfile", "requirements.txt"], tolist(fileset("${path.module}/..", "app/**")))
  docker_context_hash    = sha256(join("", [for file in sort(local.docker_context_files) : filesha256("${path.module}/../${file}")]))
}

// Build and run docker image once the repo is created
resource "terraform_data" "build_image" {
  triggers_replace = local.docker_context_hash

  // Wait on repository
  depends_on = [
    google_artifact_registry_repository.weather_etl_repo
  ]

  // Execute local gcloud command to deploy
  provisioner "local-exec" {
    command = "gcloud.cmd builds submit --project=${var.project} --region=${var.region} --tag=${local.container_image} ${path.module}/.."
  }
}