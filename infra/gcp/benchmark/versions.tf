terraform {
  required_version = ">= 1.7.0"

  backend "gcs" {
    bucket = "doss-objectkv-dev-okv-evals"
    prefix = "terraform/benchmark-runner-v1"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.42.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
