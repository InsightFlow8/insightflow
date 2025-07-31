# =============================
# Glue Table Submodule for ETL Clean Data
# Parquet 格式表配置
# =============================

resource "aws_glue_catalog_table" "table" {
  name          = var.name
  database_name = var.database_name
  description   = "ETL 清洗后的数据表 - Parquet 格式"

  # 存储描述符 - Parquet 格式
  storage_descriptor {
    location      = var.location
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # 列定义
    dynamic "columns" {
      for_each = var.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  # 表类型设置
  table_type = "EXTERNAL_TABLE"

  # 表参数
  parameters = {
    "classification"                   = "parquet"
    "compressionType"                 = "snappy"
    "typeOfData"                      = "file"
    "has_encrypted_data"              = "false"
    "parquet.compression"             = "SNAPPY"
    "projection.enabled"              = "false"
    "skip.header.line.count"          = "0"
  }
}
