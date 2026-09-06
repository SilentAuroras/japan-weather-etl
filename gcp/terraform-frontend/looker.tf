// Looker Secrets
variable "looker_id" {
  type = string
  sensitive = true
}
variable "looker_secret" {
    type = string
    sensitive = true
}

// Looker Instance
resource "google_looker_instance" "looker_weather" {
    name = "looker_weather"
    region = var.region
    platform_edition = "LOOKER_CORE_STANDARD_ANNUAL"
    deletion_policy = "FORCE"

    // Set auth - pull from terraform.tfvars
    oauth_config {
        client_id = var.looker_id
        client_secret = var.looker_secret
    }
}

// Looker URL
output "looker_url" {
  description = "Looker Instance URL"
  value = google_looker_instance.looker_weather.looker_uri
}