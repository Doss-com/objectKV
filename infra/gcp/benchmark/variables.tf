variable "project_id" {
  description = "Existing isolated GCP project used by objectKV evaluations."
  type        = string
  default     = "doss-objectkv-dev"
}

variable "region" {
  description = "Region shared by the runner and object bucket."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the first single-runner benchmark cell."
  type        = string
  default     = "us-central1-a"
}

variable "network_name" {
  description = "Existing custom VPC in the objectKV development project."
  type        = string
  default     = "objectkv-eval"
}

variable "subnet_cidr" {
  description = "Private benchmark subnet used by the runner and collector."
  type        = string
  default     = "10.77.0.0/24"
}

variable "bucket_name" {
  description = "Existing regional bucket for binaries, results, and raw telemetry."
  type        = string
  default     = "doss-objectkv-dev-okv-evals"
}

variable "create" {
  description = "Explicit cost gate. No compute resource exists unless this is true."
  type        = bool
  default     = false
}

variable "run_label" {
  description = "Short stable label used in resource names and receipts."
  type        = string
  default     = "disabled"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,19}$", var.run_label))
    error_message = "run_label must be 1 to 20 lowercase letters, digits, or hyphens."
  }
}

variable "runner_phase" {
  description = "Runner identity for a standard benchmark or one side of a sequential provider-media-loss lifecycle."
  type        = string
  default     = "standard"

  validation {
    condition     = contains(["standard", "source", "restore"], var.runner_phase)
    error_message = "runner_phase must be standard, source, or restore."
  }
}

variable "benchmark_revision" {
  description = "Clean objectKV Git revision whose binary is installed on the runner."
  type        = string
  default     = "disabled"

  validation {
    condition     = !var.create || can(regex("^[0-9a-f]{7,64}$", var.benchmark_revision))
    error_message = "benchmark_revision must be a 7 to 64 character lowercase Git SHA when create is true."
  }
}

variable "lease_expires_epoch" {
  description = "UTC epoch after which the operator must destroy the runner resources."
  type        = string
  default     = "0"

  validation {
    condition     = !var.create || can(regex("^[1-9][0-9]{9,}$", var.lease_expires_epoch))
    error_message = "lease_expires_epoch must be an explicit future UTC epoch when create is true."
  }
}

variable "runner_machine_type" {
  description = "Pinned runner shape for matched candidate and control runs."
  type        = string
  default     = "n2-standard-8"
}

variable "runner_data_disk_type" {
  description = "Persistent data-media control used by SSD serving profiles."
  type        = string
  default     = "pd-ssd"
}

variable "runner_data_disk_gib" {
  description = "Data disk size, which affects provisioned persistent-disk performance."
  type        = number
  default     = 200

  validation {
    condition     = var.runner_data_disk_gib >= 100
    error_message = "runner_data_disk_gib must be at least 100 GiB."
  }
}

variable "runner_hot_mount" {
  description = "Mount for the disposable local-NVMe serving-image tier."
  type        = string
  default     = "/var/lib/objectkv-hot"

  validation {
    condition     = startswith(var.runner_hot_mount, "/var/lib/")
    error_message = "runner_hot_mount must be an absolute path below /var/lib."
  }
}

variable "enable_local_ssd" {
  description = "Attach local NVMe scratch for serving-path benchmarks. Media-loss correctness phases disable it."
  type        = bool
  default     = true
}

variable "collector_machine_type" {
  description = "Separate machine so telemetry processing does not consume runner CPU."
  type        = string
  default     = "e2-standard-2"
}

variable "collector_image" {
  description = "Pinned OpenTelemetry Collector container tag."
  type        = string
  default     = "otel/opentelemetry-collector-contrib:0.157.0"
}

variable "operator_ssh_public_key" {
  description = "Optional break-glass SSH public key. When set, startup creates the objectkv operator account and disables OS Login on the leased machines."
  type        = string
  default     = ""

  validation {
    condition     = var.operator_ssh_public_key == "" || can(regex("^ssh-[a-z0-9-]+ ", var.operator_ssh_public_key))
    error_message = "operator_ssh_public_key must be empty or an OpenSSH public key."
  }
}
