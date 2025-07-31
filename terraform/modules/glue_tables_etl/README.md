# Glue Tables ETL Module

## 概述
此模块为 ETL 清洗后的数据创建 AWS Glue Catalog 表。用于存储和管理 after-clean 阶段的 Parquet 格式数据。

## 功能特点
- 创建专用的 Glue Catalog 数据库 `insightflow_imba_clean_data_catalog`
- 为所有数据表创建 Parquet 格式的 Glue 表
- 支持清洗后数据的 S3 路径配置
- 统一的表前缀管理

## 数据表
此模块创建以下清洗后数据表：
1. `after_clean_departments` - 部门数据
2. `after_clean_aisles` - 过道数据
3. `after_clean_products` - 产品数据
4. `after_clean_orders` - 订单数据
5. `after_clean_order_products_prior` - 历史订单产品数据
6. `after_clean_order_products_train` - 训练集订单产品数据

## 使用方法

```hcl
module "glue_tables_etl" {
  source = "./modules/glue_tables_etl"
  
  clean_database_name    = "insightflow_imba_clean_data_catalog"
  s3_clean_bucket_name   = "insightflow-dev-clean-bucket"
  after_clean_table_prefix = "after_clean_"
  env                    = "dev"
  
  tags = {
    Environment = "dev"
    Project     = "insightflow"
    Owner       = "imba-group"
  }
}
```

## 输入变量

| 变量名 | 描述 | 类型 | 默认值 | 必需 |
|--------|------|------|--------|------|
| clean_database_name | ETL 清洗后数据的 Glue Catalog 数据库名称 | string | "insightflow_imba_clean_data_catalog" | 否 |
| s3_clean_bucket_name | 存储清洗后数据的 S3 bucket 名称 | string | - | 是 |
| after_clean_table_prefix | After-clean 表的前缀 | string | "after_clean_" | 否 |
| env | 环境 (dev, staging, prod) | string | - | 是 |
| tags | 应用于资源的通用标签 | map(string) | {} | 否 |

## 输出

| 输出名 | 描述 |
|--------|------|
| clean_database_name | ETL 清洗后数据的 Glue 数据库名称 |
| clean_database_arn | ETL 清洗后数据的 Glue 数据库 ARN |
| departments_clean_table_name | Departments clean table name |
| aisles_clean_table_name | Aisles clean table name |
| products_clean_table_name | Products clean table name |
| orders_clean_table_name | Orders clean table name |
| order_products_prior_clean_table_name | Order products prior clean table name |
| order_products_train_clean_table_name | Order products train clean table name |

## 数据格式
- **存储格式**: Parquet
- **压缩**: Snappy
- **位置**: `s3://{bucket}/after-clean/{table_name}/`

## 依赖关系
- AWS Glue Catalog
- S3 Clean Bucket
- 相应的 IAM 权限

## 注意事项
- 此模块针对清洗后的 Parquet 数据设计
- 与原始数据的 CSV 格式模块 `glue_tables` 分离
- 支持后续 ETL 流程中的数据变换需求
