output "project_id" {
  value = google_project.playground.project_id
}

output "project_name" {
  value = google_project.playground.name
}

output "region" {
  value = var.region
}

output "eval_bucket" {
  value = google_storage_bucket.evals.name
}

output "eval_runner_service_account" {
  value = google_service_account.eval_runner.email
}

