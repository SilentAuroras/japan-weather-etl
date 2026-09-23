// GCS bucket for docker execution output
resource "google_storage_bucket" "japan-weather-docker-mount" {
  name                     = "japan-weather-docker-mount"
  public_access_prevention = "enforced"
  location                 = var.region
  force_destroy            = true
}