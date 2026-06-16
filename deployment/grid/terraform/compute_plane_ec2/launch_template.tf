# Copyright 2024 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 https://aws.amazon.com/apache-2-0/

# Worker launch template. ORB launches instances from an equivalent profile; this
# template encodes the proven settings (IMDSv2 hop-limit >= 2 so bridge containers can
# reach IMDS, encrypted gp3 root, the rendered user-data) and makes the worker
# independently launchable for testing.
resource "aws_launch_template" "worker" {
  name          = "htc-ec2-worker-${local.suffix}"
  image_id      = data.aws_ssm_parameter.al2023_ami.value
  instance_type = var.instance_type
  user_data     = local.user_data

  iam_instance_profile {
    arn = aws_iam_instance_profile.worker.arn
  }

  vpc_security_group_ids = [aws_security_group.worker.id]

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 3
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.instance_volume_size
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "htc-ec2-worker-${local.suffix}"
      service = "htc-aws"
    }
  }
}
