# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

variable "region" {
  description = "AWS region"
  type        = string
}

variable "suffix" {
  description = "Resource name suffix (project_name)"
  type        = string
}

variable "aws_htc_ecr" {
  description = "ECR registry URL (for the SAM build image used by terraform-aws-modules/lambda)"
  type        = string
}

variable "lambda_runtime" {
  description = "Python runtime for the ORB orchestrator Lambda (zip build)"
  type        = string
  default     = "python3.11"
}

variable "table_prefix" {
  description = "DynamoDB table prefix for ORB state; must match the bundled ORB config"
  type        = string
}

variable "worker_instance_role_arn" {
  description = "Worker instance role ARN; the orchestrator gets iam:PassRole on it (workers attach an instance profile)"
  type        = string
}

variable "worker_instance_profile_arn" {
  description = "Worker instance profile ARN ORB attaches to launched instances"
  type        = string
}

variable "worker_subnet_ids" {
  description = "Private subnet ids ORB launches workers into"
  type        = list(string)
}

variable "worker_security_group_id" {
  description = "Worker security group id"
  type        = string
}

variable "worker_ami_id" {
  description = "AL2023 AMI id for workers"
  type        = string
}

variable "worker_instance_type" {
  description = "Worker instance type"
  type        = string
}

variable "orb_template_id" {
  description = "ORB template id used for worker launches"
  type        = string
  default     = "RunInstances-OnDemand"
}

variable "worker_user_data_ssm_param" {
  description = "SSM parameter name holding the plain-text worker cloud-init (injected into the ORB template user_data)"
  type        = string
}

variable "drain_deadline_sec" {
  description = "Seconds a cordoned worker may finish in-flight work before being force-terminated on scale-down (≈ worker compose stop_grace_period)"
  type        = number
  default     = 1500
}

variable "kms_key_admin_arns" {
  description = "IAM principal ARNs allowed to administer the ORB state CMK"
  type        = list(string)
  default     = []
}

variable "kms_deletion_window" {
  description = "KMS key deletion window (days)"
  type        = number
  default     = 7
}
