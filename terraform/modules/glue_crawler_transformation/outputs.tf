output "crawler_role_arn" {
  description = "ARN of the IAM role used by the crawlers"
  value       = aws_iam_role.glue_crawler_role.arn
}

output "crawler_role_name" {
  description = "Name of the IAM role used by the crawlers"
  value       = aws_iam_role.glue_crawler_role.name
}

output "user_features_crawler_name" {
  description = "Name of the user features crawler"
  value       = aws_glue_crawler.user_features.name
}

output "product_features_crawler_name" {
  description = "Name of the product features crawler"
  value       = aws_glue_crawler.product_features.name
}

output "upi_features_crawler_name" {
  description = "Name of the UPI features crawler"
  value       = aws_glue_crawler.upi_features.name
}

output "product_features_union_crawler_name" {
  description = "Name of the product features union crawler"
  value       = aws_glue_crawler.product_features_union.name
}

output "upi_features_union_crawler_name" {
  description = "Name of the UPI features union crawler"
  value       = aws_glue_crawler.upi_features_union.name
}

output "crawler_names" {
  description = "List of all crawler names"
  value = [
    aws_glue_crawler.user_features.name,
    aws_glue_crawler.product_features.name,
    aws_glue_crawler.upi_features.name,
    aws_glue_crawler.product_features_union.name,
    aws_glue_crawler.upi_features_union.name
  ]
}
