# S3 Script Upload Resource
resource "aws_s3_object" "etl_data_clean_script" {
  bucket = var.scripts_bucket
  key    = "ETL/data_clean/ETL_data_clean.py"
  source = "../../functions/modules/ETL/data_clean/ETL_data_clean.py"
  etag   = filemd5("../../functions/modules/ETL/data_clean/ETL_data_clean.py")
  
  tags = {
    Name        = "ETL Data Clean Script"
    Environment = "dev"
    Component   = "etl-script"
  }
}

# Glue Job resource is defined in main.tf
# This file only handles script upload
