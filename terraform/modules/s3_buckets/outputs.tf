output "bucket_ids" {
  description = "IDs of the created S3 buckets"
  value = [
    for name in var.bucket_names :
    aws_s3_bucket.buckets[name].id
  ]
}

output "bucket_arns" {
  description = "ARNs of the created S3 buckets in input order"
  value = [
    for name in var.bucket_names :
    aws_s3_bucket.buckets[name].arn
  ]
}

output "raw_bucket_arn" {
  description = "ARN of the raw S3 bucket"
  value       = aws_s3_bucket.buckets[var.raw_bucket].arn
}

output "clean_bucket_arn" {
  description = "ARN of the clean S3 bucket"
  value       = aws_s3_bucket.buckets[var.clean_bucket].arn
}

output "curated_bucket_arn" {
  description = "ARN of the curated S3 bucket"
  value       = aws_s3_bucket.buckets[var.curated_bucket].arn
}
