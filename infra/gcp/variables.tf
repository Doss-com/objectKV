variable "organization_id" {
  description = "Numeric Google Cloud organization ID that owns the playground."
  type        = string
}

variable "billing_account" {
  description = "Billing account ID attached to the playground project."
  type        = string
  sensitive   = true
}

variable "project_id" {
  description = "Globally unique lowercase project ID. The display name remains objectKV-dev."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must satisfy the Google Cloud project ID naming contract."
  }
}

variable "region" {
  description = "Single region used for comparable object-store measurements."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Optional globally unique bucket name. Defaults to <project_id>-okv-evals."
  type        = string
  default     = null
}

