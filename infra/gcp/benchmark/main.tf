locals {
  resources          = var.create ? { active = var.run_label } : {}
  runner_name_suffix = var.runner_phase == "standard" ? "runner" : var.runner_phase
  runner_data_suffix = var.runner_phase == "standard" ? "data" : "${var.runner_phase}-data"
  common_labels = {
    environment = "development"
    managed_by  = "terraform"
    project     = "objectkv"
    purpose     = "benchmark"
    run         = var.run_label
    expires     = var.lease_expires_epoch
  }
}

data "google_compute_network" "eval" {
  name    = var.network_name
  project = var.project_id
}

data "google_compute_image" "runner" {
  family  = "debian-12"
  project = "debian-cloud"
}

data "google_compute_image" "collector" {
  family  = "cos-stable"
  project = "cos-cloud"
}

data "google_service_account" "eval_runner" {
  account_id = "objectkv-eval-runner"
  project    = var.project_id
}

resource "google_compute_subnetwork" "benchmark" {
  for_each = local.resources

  name                     = "objectkv-benchmark-v1"
  project                  = var.project_id
  region                   = var.region
  network                  = data.google_compute_network.eval.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true

  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

resource "google_compute_router" "benchmark" {
  for_each = local.resources

  name    = "objectkv-benchmark-v1"
  project = var.project_id
  region  = var.region
  network = data.google_compute_network.eval.id
}

resource "google_compute_router_nat" "benchmark" {
  for_each = local.resources

  name                               = "objectkv-benchmark-v1"
  project                            = var.project_id
  region                             = var.region
  router                             = google_compute_router.benchmark[each.key].name
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.benchmark[each.key].id
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

resource "google_compute_firewall" "iap_ssh" {
  for_each = local.resources

  name      = "objectkv-benchmark-iap-ssh-v1"
  project   = var.project_id
  network   = data.google_compute_network.eval.name
  direction = "INGRESS"
  priority  = 1000

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["objectkv-benchmark"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "runner_to_collector" {
  for_each = local.resources

  name      = "objectkv-benchmark-otlp-v1"
  project   = var.project_id
  network   = data.google_compute_network.eval.name
  direction = "INGRESS"
  priority  = 1000

  source_tags = ["objectkv-benchmark-runner"]
  target_tags = ["objectkv-benchmark-collector"]

  allow {
    protocol = "tcp"
    ports    = ["4317", "4318", "13133"]
  }
}

resource "google_compute_disk" "runner_data" {
  for_each = local.resources

  name    = "objectkv-bench-${each.value}-${local.runner_data_suffix}"
  project = var.project_id
  zone    = var.zone
  type    = var.runner_data_disk_type
  size    = var.runner_data_disk_gib
  labels  = local.common_labels

  physical_block_size_bytes = 4096
}

resource "google_compute_disk" "collector_data" {
  for_each = local.resources

  name    = "objectkv-bench-${each.value}-otel"
  project = var.project_id
  zone    = var.zone
  type    = "pd-balanced"
  size    = 20
  labels  = local.common_labels

  physical_block_size_bytes = 4096
}

resource "google_compute_instance" "runner" {
  for_each = local.resources

  name         = "objectkv-bench-${each.value}-${local.runner_name_suffix}"
  project      = var.project_id
  zone         = var.zone
  machine_type = var.runner_machine_type
  tags         = ["objectkv-benchmark", "objectkv-benchmark-runner"]
  labels       = local.common_labels

  boot_disk {
    auto_delete = true
    initialize_params {
      image = data.google_compute_image.runner.self_link
      size  = 30
      type  = "pd-balanced"
    }
  }

  attached_disk {
    source      = google_compute_disk.runner_data[each.key].id
    device_name = "objectkv-data"
    mode        = "READ_WRITE"
  }

  # Disposable complete serving images live here. The persistent pd-ssd above
  # remains available for stable-media controls and receipts.
  scratch_disk {
    interface = "NVME"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.benchmark[each.key].id
  }

  metadata = merge({
    enable-oslogin          = var.operator_ssh_public_key == "" ? "TRUE" : "FALSE"
    block-project-ssh-keys  = "TRUE"
    objectkv-run-label      = var.run_label
    objectkv-runner-phase   = var.runner_phase
    objectkv-revision       = var.benchmark_revision
    objectkv-lease-expires  = var.lease_expires_epoch
    objectkv-results-bucket = var.bucket_name
    objectkv-hot-mount      = var.runner_hot_mount
    startup-script          = file("${path.module}/runner-startup.sh")
    }, var.operator_ssh_public_key == "" ? {} : {
    objectkv-operator-ssh-key = var.operator_ssh_public_key
  })

  service_account {
    email  = data.google_service_account.eval_runner.email
    scopes = ["cloud-platform"]
  }

  allow_stopping_for_update = true
  deletion_protection       = false

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  depends_on = [google_compute_router_nat.benchmark]
}

resource "google_compute_instance" "collector" {
  for_each = local.resources

  name         = "objectkv-bench-${each.value}-collector"
  project      = var.project_id
  zone         = var.zone
  machine_type = var.collector_machine_type
  tags         = ["objectkv-benchmark", "objectkv-benchmark-collector"]
  labels       = local.common_labels

  boot_disk {
    auto_delete = true
    initialize_params {
      image = data.google_compute_image.collector.self_link
      size  = 20
      type  = "pd-balanced"
    }
  }

  attached_disk {
    source      = google_compute_disk.collector_data[each.key].id
    device_name = "objectkv-otel"
    mode        = "READ_WRITE"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.benchmark[each.key].id
  }

  metadata = merge({
    enable-oslogin           = var.operator_ssh_public_key == "" ? "TRUE" : "FALSE"
    block-project-ssh-keys   = "TRUE"
    objectkv-run-label       = var.run_label
    objectkv-revision        = var.benchmark_revision
    objectkv-lease-expires   = var.lease_expires_epoch
    objectkv-collector-image = var.collector_image
    objectkv-runner-cidr     = var.subnet_cidr
    objectkv-otel-config     = base64encode(file("${path.module}/otel-collector.yaml"))
    startup-script           = file("${path.module}/collector-startup.sh")
    }, var.operator_ssh_public_key == "" ? {} : {
    objectkv-operator-ssh-key = var.operator_ssh_public_key
  })

  service_account {
    email  = data.google_service_account.eval_runner.email
    scopes = ["cloud-platform"]
  }

  allow_stopping_for_update = true
  deletion_protection       = false

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  depends_on = [google_compute_router_nat.benchmark]
}
