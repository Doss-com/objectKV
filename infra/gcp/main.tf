provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bucket_name = coalesce(var.bucket_name, "${var.project_id}-okv-evals")
  services = toset([
    "cloudbilling.googleapis.com",
    "compute.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudtrace.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_compute_network" "eval" {
  name                    = "objectkv-eval"
  project                 = google_project.playground.project_id
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"

  depends_on = [google_project_service.enabled["compute.googleapis.com"]]
}

resource "google_compute_subnetwork" "eval" {
  name                     = "objectkv-eval-us-central1"
  project                  = google_project.playground.project_id
  region                   = var.region
  network                  = google_compute_network.eval.id
  ip_cidr_range            = "10.41.0.0/24"
  private_ip_google_access = true
}

resource "google_project" "playground" {
  name                = "objectKV-dev"
  project_id          = var.project_id
  org_id              = var.organization_id
  billing_account     = var.billing_account
  auto_create_network = false

  labels = {
    environment = "development"
    managed_by  = "terraform"
    project     = "objectkv"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_service" "enabled" {
  for_each = local.services

  project            = google_project.playground.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "evals" {
  name                        = local.bucket_name
  project                     = google_project.playground.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  labels = {
    environment = "development"
    managed_by  = "terraform"
    project     = "objectkv"
    purpose     = "database-evals"
  }

  versioning {
    enabled = true
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["scratch/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.enabled["storage.googleapis.com"]]

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "eval_runner" {
  project      = google_project.playground.project_id
  account_id   = "objectkv-eval-runner"
  display_name = "objectKV eval runner"

  depends_on = [google_project_service.enabled["cloudresourcemanager.googleapis.com"]]
}

resource "google_storage_bucket_iam_member" "eval_runner_objects" {
  bucket = google_storage_bucket.evals.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.eval_runner.email}"
}

resource "google_project_iam_member" "eval_runner_metrics" {
  project = google_project.playground.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.eval_runner.email}"
}

resource "google_project_iam_member" "eval_runner_logs" {
  project = google_project.playground.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.eval_runner.email}"
}

resource "google_project_iam_member" "eval_runner_traces" {
  project = google_project.playground.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.eval_runner.email}"
}
