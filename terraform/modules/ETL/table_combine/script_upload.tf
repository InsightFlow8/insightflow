# S3 Script Upload Resource
resource "aws_s3_object" "etl_table_combine_script" {
  bucket = var.scripts_bucket
  key    = "ETL/table_combine/ETL_table_combine.py"
  source = "../../functions/modules/ETL/table_combine/ETL_table_combine.py"
  etag   = filemd5("../../functions/modules/ETL/table_combine/ETL_table_combine.py")
  
  tags = {
    Name        = "ETL Table Combine Script"
    Environment = "dev"
    Component   = "etl-script"
  }
}
