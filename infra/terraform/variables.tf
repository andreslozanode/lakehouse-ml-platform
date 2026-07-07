variable "environment" {
  description = "Deployment environment (dev | qa | prod)."
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "environment must be dev, qa or prod."
  }
}

variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "enable_msk" {
  description = "Provision an MSK cluster for CDC (local docker-compose is used when false)."
  type        = bool
  default     = false
}

variable "msk_broker_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "msk_subnet_ids" {
  description = "Private subnets for MSK brokers (required when enable_msk = true)."
  type        = list(string)
  default     = []
}

variable "lifecycle_expiration_days" {
  description = "Days before landing-zone objects expire."
  type        = number
  default     = 30
}
