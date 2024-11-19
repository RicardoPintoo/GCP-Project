# Project ID for GCP
variable "project_id" {
  description = "The Google Cloud project ID where resources will be created"
  type        = string
  default = "moonlit-app-441813-v9"
}

# Region for GCP resources
variable "region" {
  description = "The Google Cloud region where resources will be created"
  type        = string
  default     = "europe-west1"
}

# Environment (optional, for naming conventions)
variable "environment" {
  description = "The environment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}
