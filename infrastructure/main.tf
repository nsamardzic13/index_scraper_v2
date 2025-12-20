terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.6.0"
    }
  }
}

# Test commit to trigger GitHub Actions workflow

provider "google" {
  credentials = local.credentials_file
  project     = var.gcp_project
  region      = var.region
}
