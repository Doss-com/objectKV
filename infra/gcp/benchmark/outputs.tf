output "run_label" {
  value = var.create ? var.run_label : null
}

output "runner_phase" {
  value = var.create ? var.runner_phase : null
}

output "runner_name" {
  value = try(google_compute_instance.runner["active"].name, null)
}

output "runner_internal_ip" {
  value = try(google_compute_instance.runner["active"].network_interface[0].network_ip, null)
}

output "runner_data_disk_name" {
  value = try(google_compute_disk.runner_data["active"].name, null)
}

output "runner_data_disk_id" {
  value = try(google_compute_disk.runner_data["active"].id, null)
}

output "collector_name" {
  value = try(google_compute_instance.collector["active"].name, null)
}

output "collector_internal_ip" {
  value = try(google_compute_instance.collector["active"].network_interface[0].network_ip, null)
}

output "otlp_http_endpoint" {
  value = try("http://${google_compute_instance.collector["active"].network_interface[0].network_ip}:4318", null)
}

output "resolved_runner_image" {
  value = data.google_compute_image.runner.self_link
}

output "resolved_collector_image" {
  value = data.google_compute_image.collector.self_link
}

output "destroy_command" {
  value = var.create ? "terraform -chdir=infra/gcp/benchmark destroy -var=create=true -var=run_label=${var.run_label} -var=runner_phase=${var.runner_phase} -var=enable_local_ssd=${var.enable_local_ssd} -var=benchmark_revision=${var.benchmark_revision} -var=lease_expires_epoch=${var.lease_expires_epoch}" : null
}
