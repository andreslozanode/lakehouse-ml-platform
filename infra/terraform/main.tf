locals {
  layers = ["landing", "bronze", "silver", "gold"]
}

# ---- Lakehouse storage: one versioned, encrypted bucket per medallion layer ----
resource "aws_s3_bucket" "layer" {
  for_each = toset(local.layers)
  bucket   = "lakehouse-ml-${var.environment}-${each.key}"
}

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each                = aws_s3_bucket.layer
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing_expiry" {
  bucket = aws_s3_bucket.layer["landing"].id
  rule {
    id     = "expire-raw-landing"
    status = "Enabled"
    expiration { days = var.lifecycle_expiration_days }
  }
}

# ---- Least-privilege pipeline role (assumed by Databricks / CI runners) ----
data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "lakehouse-ml-${var.environment}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume.json
}

data "aws_iam_policy_document" "pipeline_access" {
  statement {
    sid       = "LakehouseObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in aws_s3_bucket.layer : "${b.arn}/*"]
  }
  statement {
    sid       = "LakehouseList"
    actions   = ["s3:ListBucket"]
    resources = [for b in aws_s3_bucket.layer : b.arn]
  }
}

resource "aws_iam_role_policy" "pipeline" {
  name   = "lakehouse-access"
  role   = aws_iam_role.pipeline.id
  policy = data.aws_iam_policy_document.pipeline_access.json
}

# ---- Optional MSK for cloud CDC ----
resource "aws_msk_cluster" "cdc" {
  count                  = var.enable_msk ? 1 : 0
  cluster_name           = "lakehouse-cdc-${var.environment}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type  = var.msk_broker_instance_type
    client_subnets = var.msk_subnet_ids
    storage_info {
      ebs_storage_info { volume_size = 100 }
    }
  }

  encryption_info {
    encryption_in_transit { client_broker = "TLS" }
  }
}
