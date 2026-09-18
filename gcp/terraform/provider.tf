// Setup providers
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.44.0"
    }
  }
}

// Set GCP provider
provider "google" {
  region = var.region
  project = var.project
}

// Project name
variable "project" {
  type = string
}

// Region
variable "region" {
  type = string
}

// Location
variable "location" {
  type = string
}

// Overpass API referer
variable "overpass_referer" {
  type = string
}

// Overpass API user agent
variable "overpass_user_agent" {
  type = string
}