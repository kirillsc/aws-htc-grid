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

variable "lambda_runtime" {
  description = "Python runtime for the controller Lambda"
  type        = string
}

variable "aws_htc_ecr" {
  description = "ECR registry URL (for the SAM build image used by terraform-aws-modules/lambda)"
  type        = string
}

variable "orchestrator_function_name" {
  description = "ORB orchestrator Lambda function name the controller invokes"
  type        = string
}

variable "orchestrator_function_arn" {
  description = "ORB orchestrator Lambda ARN (for the lambda:InvokeFunction grant)"
  type        = string
}

variable "orb_template_id" {
  description = "ORB template id used for scale-up"
  type        = string
  default     = "RunInstances-OnDemand"
}

variable "metric_namespace" {
  description = "CloudWatch namespace of the backlog metric (pending_tasks_ddb)"
  type        = string
}

variable "metric_name" {
  description = "CloudWatch metric name for the backlog"
  type        = string
}

variable "metric_dimension_name" {
  description = "Backlog metric dimension name"
  type        = string
}

variable "metric_dimension_value" {
  description = "Backlog metric dimension value (cluster name)"
  type        = string
}

variable "min_instances" {
  description = "Minimum worker instances"
  type        = number
  default     = 0
}

variable "max_instances" {
  description = "Maximum worker instances"
  type        = number
  default     = 5
}

variable "target_pending_per_instance" {
  description = "Target pending tasks per instance (~2 * NUM_PAIRS)"
  type        = number
  default     = 4
}

variable "control_interval" {
  description = "Controller reconcile interval (seconds)"
  type        = number
  default     = 60
}

variable "drain_deadline_sec" {
  description = "Seconds a cordoned worker may finish in-flight work before being force-terminated on graceful scale-down (≈ worker compose stop_grace_period)"
  type        = number
  default     = 1500
}

variable "state_table_name" {
  description = "DynamoDB task state table name (read for the live-task heartbeat busy-worker detection)"
  type        = string
}

variable "state_table_arn" {
  description = "DynamoDB task state table ARN (for the dynamodb:Query grant on the table + its GSIs)"
  type        = string
}

variable "state_table_kms_key_arn" {
  description = "KMS CMK ARN encrypting the state table (kms:Decrypt is required to Query the encrypted table)"
  type        = string
}

variable "state_table_service" {
  description = "State table backend service (DynamoDB)"
  type        = string
  default     = "DynamoDB"
}

variable "state_table_config" {
  description = "State table client config JSON (e.g. retries)"
  type        = string
  default     = "{}"
}

variable "kms_key_admin_arns" {
  description = "IAM principal ARNs allowed to administer the controller CloudWatch logs CMK"
  type        = list(string)
  default     = []
}

variable "kms_deletion_window" {
  description = "KMS key deletion window (days)"
  type        = number
  default     = 7
}
