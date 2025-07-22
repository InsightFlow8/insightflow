# =============================
# 简化版增量爬取模块输出
# =============================

output "glue_database_name" {
  description = "简化版Glue数据库名称"
  value       = aws_glue_catalog_database.raw_data_catalog.name
}

output "glue_database_arn" {
  description = "Glue数据库ARN"
  value       = aws_glue_catalog_database.raw_data_catalog.arn
}

output "glue_role_arn" {
  description = "Glue爬虫IAM角色ARN"
  value       = aws_iam_role.glue_crawler_role.arn
}

output "crawler_names" {
  description = "所有爬虫名称"
  value = {
    orders               = aws_glue_crawler.raw_orders.name
    products             = aws_glue_crawler.raw_products.name
    departments          = aws_glue_crawler.raw_departments.name
    aisles               = aws_glue_crawler.raw_aisles.name
    order_products_prior = aws_glue_crawler.raw_order_products_prior.name
    order_products_train = aws_glue_crawler.raw_order_products_train.name
  }
}

output "table_names" {
  description = "爬虫将创建的表名称"
  value = {
    orders               = "${var.table_prefix}orders"
    products             = "${var.table_prefix}products"
    departments          = "${var.table_prefix}departments"
    aisles               = "${var.table_prefix}aisles"
    order_products_prior = "${var.table_prefix}order_products_prior"
    order_products_train = "${var.table_prefix}order_products_train"
  }
}

# 增量爬取相关输出
output "crawl_behavior" {
  description = "当前使用的爬取策略"
  value       = var.recrawl_behavior
}

output "crawler_schedule" {
  description = "爬虫运行调度"
  value       = var.crawler_schedule
}

# 用于DMS集成的Glue表ARN
output "glue_table_arns" {
  description = "用于DMS集成的Glue表ARN列表"
  value = {
    orders               = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}orders"
    products             = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}products"
    departments          = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}departments"
    aisles               = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}aisles"
    order_products_prior = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}order_products_prior"
    order_products_train = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.raw_data_catalog.name}/${var.table_prefix}order_products_train"
  }
}

output "s3_placeholder_objects" {
  description = "S3占位符对象信息"
  value = {
    for table, obj in aws_s3_object.glue_crawler_placeholders : table => {
      bucket = obj.bucket
      key    = obj.key
      etag   = obj.etag
    }
  }
}
