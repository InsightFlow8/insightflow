# ETL Table Combine Module

This Terraform module creates AWS Glue resources for the ETL table combination stage.

## Purpose

This module combines cleaned data tables into a single large table for downstream transformations:
- Reads cleaned parquet files from S3
- Joins multiple tables (aisles, departments, products, orders, order_products)
- Outputs combined table partitioned by `eval_set`

## Resources Created

- AWS Glue Job for table combination
- IAM Role with necessary permissions
- CloudWatch Log Group for job logging
- S3 script upload

## Input Data

The module expects cleaned parquet files from the data_clean stage:
- `s3://.../temp/aisles/`
- `s3://.../temp/departments/`
- `s3://.../temp/products/`
- `s3://.../temp/orders/`
- `s3://.../temp/order_products_prior/`
- `s3://.../temp/order_products_train/`

## Output Data

Combined table saved to:
- `s3://.../combined/eval_set=*/part-*.parquet`

## Usage

```hcl
module "etl_table_combine" {
  source = "./modules/ETL/table_combine"
  
  job_name        = "insightflow-table-combine-job"
  iam_role_name   = "insightflow-glue-table-combine-role"
  scripts_bucket  = "insightflow-dev-scripts"
  temp_dir        = "s3://insightflow-dev-clean-bucket/glue-temp/"
  
  # Input paths (from data_clean stage)
  aisles_path                  = "s3://insightflow-dev-clean-bucket/temp/aisles/"
  departments_path             = "s3://insightflow-dev-clean-bucket/temp/departments/"
  products_path                = "s3://insightflow-dev-clean-bucket/temp/products/"
  orders_path                  = "s3://insightflow-dev-clean-bucket/temp/orders/"
  order_products_prior_path    = "s3://insightflow-dev-clean-bucket/temp/order_products_prior/"
  order_products_train_path    = "s3://insightflow-dev-clean-bucket/temp/order_products_train/"
  
  # Output path
  output_path = "s3://insightflow-dev-clean-bucket/combined/"
  
  tags = {
    Project     = "InsightFlow"
    Environment = "dev"
    Component   = "etl-table-combine"
  }
}
```

## Dependencies

- S3 buckets must exist
- Input data from ETL data_clean stage must be available
