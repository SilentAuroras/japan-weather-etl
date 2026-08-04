// Setup providers
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.10.0"
    }
  }
}

// Project name - pull from terraform.tfvars
variable "project" {
  type = string
}

// Region - pull from terraform.tfvars
variable "region" {
  type = string
}

// Location - pull from terraform.tfvars
variable "location" {
  type = string
}

// Set GCP provider variables
provider "google" {
  region = var.region
  project = var.project
}