# S3 Script Upload Resource
resource "aws_s3_object" "etl_data_transformation_script" {
  bucket = var.scripts_bucket
  key    = "ETL/data_transformation/ETL_data_transformation.py"
  source = "../../functions/modules/ETL/data_transformation/ETL_data_transformation.py"
  etag   = filemd5("../../functions/modules/ETL/data_transformation/ETL_data_transformation.py")
  
  tags = {
    Name        = "ETL Data Transformation Script"
    Environment = "dev"
    Component   = "etl-script"
  }
}

# Glue Job resource is defined in main.tf
# This file only handles script upload
