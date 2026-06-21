# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

# ORB orchestrator: the fleet-scaling orchestrator for the ec2 backend. Ports the proven
# CDK PoC (deployment/orb-poc/cdk/orb_poc_stack.py) to Terraform:
#   - 3 DynamoDB tables (machines/requests/templates) with the exact `id`:S schema ORB
#     DescribeTable-checks and skips its own CreateTable;
#   - a CMK encrypting them;
#   - a ZIP-packaged Lambda (orb-py + 4 patches + handler + config), outside any VPC, built in
#     the SAM build container (consistent with the other htc-grid Lambdas — no Docker image/ECR);
#   - a least-privilege role: DDB RW on the 3 tables, EC2 launch-template + run/terminate/
#     describe, SSM AMI read, KMS use, and iam:PassRole on the worker instance role.

locals {
  account_id = data.aws_caller_identity.current.account_id
  dns_suffix = data.aws_partition.current.dns_suffix
  partition  = data.aws_partition.current.partition

  tables = ["machines", "requests", "templates"]

  lambda_build_runtime = "${var.aws_htc_ecr}/ecr-public/sam/build-${var.lambda_runtime}:1"
  orb_source_dir       = "../../../source/compute_plane/orb_orchestrator"
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# --- CMK for the 3 state tables -------------------------------------------------
module "orb_state_kms_key" {
  source  = "terraform-aws-modules/kms/aws"
  version = "~> 2.0"

  description             = "CMK for HTC-Grid ORB orchestrator DynamoDB state tables"
  deletion_window_in_days = var.kms_deletion_window
  enable_key_rotation     = true
  key_administrators      = var.kms_key_admin_arns

  aliases = ["dynamodb/orb-orchestrator-${var.suffix}"]
}

# --- 3 DynamoDB state tables (PK id:S, on-demand, PITR, CMK) ---------------------
resource "aws_dynamodb_table" "orb_state" {
  for_each = toset(local.tables)

  name         = "${var.table_prefix}-${each.value}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = module.orb_state_kms_key.key_arn
  }

  tags = {
    service = "htc-aws"
  }
}

# --- Execution policy (attached by the lambda module to the role it creates) ----
# NOTE: the ec2 launch-template + RunInstances/Describe* statements use Resource "*" by necessity:
# CreateLaunchTemplate/RunInstances/ec2:Describe* are not resource-scopable pre-creation (and the
# Describe* actions reject any ARN). Tightening is possible only via a condition (e.g. restrict
# RunInstances/TerminateInstances to instances tagged for this grid) — deferred as future hardening;
# ORB only ever acts on the instances it launches. DynamoDB/KMS/SSM/PassRole below ARE scoped.
resource "aws_iam_policy" "orb_orchestrator" {
  name        = "orb-orchestrator-${var.suffix}"
  description = "ORB orchestrator: DynamoDB state, EC2 launch/terminate, SSM AMI, KMS, PassRole worker"
  policy      = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "OrbStateTables",
      "Action": [
        "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
        "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:Scan",
        "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem", "dynamodb:DescribeTable"
      ],
      "Resource": ${jsonencode([for t in aws_dynamodb_table.orb_state : t.arn])},
      "Effect": "Allow"
    },
    {
      "Sid": "OrbLaunchTemplate",
      "Action": [
        "ec2:CreateLaunchTemplate", "ec2:CreateLaunchTemplateVersion",
        "ec2:DeleteLaunchTemplate", "ec2:DescribeLaunchTemplates",
        "ec2:DescribeLaunchTemplateVersions", "ec2:CreateTags"
      ],
      "Resource": "*",
      "Effect": "Allow"
    },
    {
      "Sid": "OrbInstances",
      "Action": [
        "ec2:RunInstances", "ec2:TerminateInstances", "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus", "ec2:DescribeImages", "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups"
      ],
      "Resource": "*",
      "Effect": "Allow"
    },
    {
      "Sid": "OrbAmiSsm",
      "Action": ["ssm:GetParameter", "ssm:GetParameters"],
      "Resource": "arn:${local.partition}:ssm:${var.region}::parameter/aws/service/ami-amazon-linux-latest/*",
      "Effect": "Allow"
    },
    {
      "Sid": "WorkerUserDataSsm",
      "Action": ["ssm:GetParameter"],
      "Resource": "arn:${local.partition}:ssm:${var.region}:${local.account_id}:parameter${var.worker_user_data_ssm_param}",
      "Effect": "Allow"
    },
    {
      "Sid": "OrbStateKms",
      "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
      "Resource": "${module.orb_state_kms_key.key_arn}",
      "Effect": "Allow"
    },
    {
      "Sid": "OrbPassWorkerRole",
      "Action": ["iam:PassRole"],
      "Resource": "${var.worker_instance_role_arn}",
      "Effect": "Allow"
    }
  ]
}
EOF
}

# --- The ZIP-packaged Lambda ----------------------------------------------------
# Built in the SAM build container (build_in_docker), like the other htc-grid Lambdas.
# The build: bundles orb_lambda.py + config/, pip-installs orb-py into the package, then
# runs the 4 mandatory orb-py DynamoDB-backend patches against the installed package before
# zipping. ORB_CONFIG_DIR points at the bundled config; ORB_*_DIR writable dirs live in /tmp.
module "orb_orchestrator" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 5.0"

  function_name = "orb-orchestrator-${var.suffix}"
  handler       = "orb_lambda.handler"
  runtime       = var.lambda_runtime
  timeout       = 300
  memory_size   = 512

  build_in_docker = true
  docker_image    = local.lambda_build_runtime
  docker_additional_options = [
    "--platform", "linux/amd64",
  ]

  # Native pip build (runs INSIDE the SAM build container, so orb-py's native wheels match the
  # Lambda runtime). The 4 mandatory orb-py DynamoDB patches are applied at COLD START by
  # orb_lambda.py against the deployed /var/task package (idempotent string-replaces) — we do NOT
  # patch at build time, because custom `commands` run on the HOST (not in docker) and would hang
  # the host pip-install. config/ is bundled under orb-config/.
  source_path = [
    {
      path             = local.orb_source_dir
      pip_requirements = true # requirements.txt found in `path`
      patterns = [
        "orb_lambda.py",
        "patches/.*",
        "!.*__pycache__.*",
        "!.*\\.pyc",
        "!\\.gitignore",
        "!docs/.*",
        "!config/.*",
        "!requirements\\.txt",
      ]
    },
    {
      path          = "${local.orb_source_dir}/config"
      prefix_in_zip = "orb-config"
    }
  ]

  role_name          = "role_orb_orchestrator_${var.suffix}"
  role_description   = "ORB orchestrator Lambda role"
  attach_policies    = true
  number_of_policies = 1
  policies           = [aws_iam_policy.orb_orchestrator.arn]

  attach_tracing_policy = true
  tracing_mode          = "Active"

  # Grid-specific values reach ORB via its own ORB_AWS_* env-var layer at cold start
  # (orb_lambda._materialize_grid_config). orb-py's AWSProviderConfig is a pydantic-settings
  # BaseSettings (env_prefix="ORB_AWS_", env_nested_delimiter="__"), so ORB_AWS_REGION and
  # ORB_AWS_STORAGE__DYNAMODB__* are consumed by ORB DIRECTLY — the bundled config.json
  # deliberately omits region/table_prefix so these env vars win (a value in the file would be an
  # init kwarg that, by pydantic-settings precedence, beats the env var). The template-only vars
  # (subnet/SG/profile/AMI/type/user_data) have no field on AWSProviderConfig, so the handler
  # substitutes them into aws_templates.json itself; they keep the ORB_AWS_ prefix only for naming
  # consistency. ORB_CONFIG_DIR is the bundled config; writable ORB dirs go under /tmp.
  # ORB_ALLOW_TERMINATE_ALL is left UNSET so the fleet-wide kill switch is disabled.
  environment_variables = {
    # Powertools structured logging: service name groups records; level is env-driven.
    POWERTOOLS_SERVICE_NAME = "orb_orchestrator"
    LOG_LEVEL               = "INFO"
    ORB_CONFIG_DIR          = "/var/task/orb-config"
    ORB_PROVIDER            = "aws"
    ORB_ROOT_DIR            = "/tmp/orb"
    ORB_WORK_DIR            = "/tmp/orb/work"
    ORB_LOG_DIR             = "/tmp/orb/logs"
    ORB_CACHE_DIR           = "/tmp/orb/cache"
    ORB_SCRIPTS_DIR         = "/tmp/orb/scripts"
    ORB_HEALTH_DIR          = "/tmp/orb/health"

    # Consumed by orb-py's AWSProviderConfig BaseSettings directly (no handler substitution).
    ORB_AWS_REGION                          = var.region
    ORB_AWS_STORAGE__DYNAMODB__TABLE_PREFIX = var.table_prefix
    ORB_AWS_STORAGE__DYNAMODB__REGION       = var.region

    # Substituted into aws_templates.json by the handler (no ORB_AWS_* field exists for these).
    ORB_AWS_TEMPLATE_ID          = var.orb_template_id
    ORB_AWS_SUBNET_IDS           = join(",", var.worker_subnet_ids)
    ORB_AWS_SECURITY_GROUP_IDS   = var.worker_security_group_id
    ORB_AWS_INSTANCE_PROFILE_ARN = var.worker_instance_profile_arn
    ORB_AWS_IMAGE_ID             = var.worker_ami_id
    ORB_AWS_INSTANCE_TYPE        = var.worker_instance_type
    ORB_AWS_USER_DATA_SSM_PARAM  = var.worker_user_data_ssm_param
  }

  tags = {
    service = "htc-aws"
  }

  depends_on = [aws_dynamodb_table.orb_state]
}
