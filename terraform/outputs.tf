output "landing_bucket_name" {
  value = aws_s3_bucket.landing_bucket.id
}

output "raw_orders_table_name" {
  value = aws_glue_crawler.raw_orders.name
}

output "raw_products_table_name" {
  value = aws_glue_crawler.raw_products.name
}

output "raw_departments_table_name" {
  value = aws_glue_crawler.raw_departments.name
}

output "raw_aisles_table_name" {
  value = aws_glue_crawler.raw_aisles.name
}