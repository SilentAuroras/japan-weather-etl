// Setup providers
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.10.0"
    }
  }
}

// Pull from terraform.tfvars
variable "project" {
  type = string
}

// Pull from terraform.tfvars
variable "region" {
  type = string
}

// Pull from terraform.tfvars
variable "location" {
  type = string
}

// Set GCP variables
provider "google" {
  region = var.region
  project = var.project
}