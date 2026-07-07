output "bucket_uris" {
  description = "s3:// URIs per medallion layer (feed LAKEHOUSE_ROOT from these)."
  value       = { for layer, bucket in aws_s3_bucket.layer : layer => "s3://${bucket.bucket}" }
}

output "pipeline_role_arn" {
  value = aws_iam_role.pipeline.arn
}

output "msk_bootstrap_brokers" {
  value     = var.enable_msk ? aws_msk_cluster.cdc[0].bootstrap_brokers_tls : null
  sensitive = true
}
