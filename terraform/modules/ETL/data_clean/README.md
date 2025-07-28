# ETL Data Clean Glue Job Module

This Terraform module creates an AWS Glue Job for data cleaning ETL operations.

## Features

- Creates AWS Glue Job with parameterized input/output paths
- Automatically creates IAM role with necessary permissions
- Supports CloudWatch logging and metrics
- Configurable worker types and scaling

## Usage

```hcl
module "etl_data_clean" {
  source = "./modules/ETL/data_clean"

  # Job Configuration
  job_name        = "insightflow-data-clean-job"
  iam_role_name   = "insightflow-glue-data-clean-role"
  script_location = "s3://insightflow-dev-scripts/ETL/data_clean/"
  temp_dir        = "s3://your-temp-bucket/glue-temp/"

  # Input S3 Paths
  aisles_path                  = "s3://insightflow-dev-raw-bucket/data/batch/aisles/"
  departments_path             = "s3://insightflow-dev-raw-bucket/data/batch/departments/"
  products_path                = "s3://insightflow-dev-raw-bucket/data/batch/products/"
  orders_path                  = "s3://insightflow-dev-raw-bucket/data/batch/orders/"
  order_products_prior_path    = "s3://insightflow-dev-raw-bucket/data/batch/order_products_prior/"
  order_products_train_path    = "s3://insightflow-dev-raw-bucket/data/batch/order_products_train/"

  # Output S3 Paths
  aisles_out                   = "s3://insightflow-dev-clean-bucket/temp/aisles/"
  departments_out              = "s3://insightflow-dev-clean-bucket/temp/departments/"
  products_out                 = "s3://insightflow-dev-clean-bucket/temp/products/"
  orders_out                   = "s3://insightflow-dev-clean-bucket/temp/orders/"
  order_products_prior_out     = "s3://insightflow-dev-clean-bucket/temp/order_products_prior/"
  order_products_train_out     = "s3://insightflow-dev-clean-bucket/temp/order_products_train/"

  # Glue Configuration
  glue_version      = "4.0"
  number_of_workers = 5
  worker_type       = "G.1X"

  tags = {
    Environment = "dev"
    Project     = "insightflow"
    Component   = "etl-data-clean"
  }
}
```

## Requirements

- Terraform >= 1.0
- AWS Provider >= 4.0

## IAM Permissions

The module automatically creates an IAM role with the following permissions:
- `AWSGlueServiceRole` - Basic Glue service permissions
- `AmazonS3FullAccess` - S3 read/write access
- `CloudWatchLogsFullAccess` - CloudWatch logging

## Outputs

- `glue_job_name` - Name of the created Glue Job
- `glue_job_arn` - ARN of the created Glue Job
- `iam_role_arn` - ARN of the created IAM Role
- `iam_role_name` - Name of the created IAM Role
