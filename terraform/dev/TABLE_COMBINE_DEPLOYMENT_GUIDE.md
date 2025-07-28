# ETL Table Combine Module Deployment Guide

## 模块概述

ETL Table Combine模块用于将已清洗的多个小表合并成一张大表，作为后续各表转置的输入数据源。

## 功能特性

- **输入数据**: 读取data_clean阶段输出的parquet文件
- **表合并**: 将6张表join成一张大表
- **性能优化**: 使用broadcast join优化小表连接
- **分区存储**: 按eval_set字段分区存储，便于后续查询
- **自动化部署**: 完整的Terraform配置

## 前置条件

1. **ETL data_clean模块已部署并运行成功**
2. **S3 buckets已存在**: 
   - `insightflow-dev-clean-bucket` (存储数据)
   - `insightflow-dev-scripts` (存储脚本)
3. **清洗后的数据已存在于以下路径**:
   ```
   s3://insightflow-dev-clean-bucket/temp/aisles/
   s3://insightflow-dev-clean-bucket/temp/departments/
   s3://insightflow-dev-clean-bucket/temp/products/
   s3://insightflow-dev-clean-bucket/temp/orders/
   s3://insightflow-dev-clean-bucket/temp/order_products_prior/
   s3://insightflow-dev-clean-bucket/temp/order_products_train/
   ```

## 部署步骤

### 1. 验证前置条件
```bash
# 检查清洗后的数据是否存在
aws s3 ls s3://insightflow-dev-clean-bucket/temp/aisles/
aws s3 ls s3://insightflow-dev-clean-bucket/temp/departments/
aws s3 ls s3://insightflow-dev-clean-bucket/temp/products/
aws s3 ls s3://insightflow-dev-clean-bucket/temp/orders/
aws s3 ls s3://insightflow-dev-clean-bucket/temp/order_products_prior/
aws s3 ls s3://insightflow-dev-clean-bucket/temp/order_products_train/
```

### 2. 部署table_combine模块
```bash
cd terraform/dev
terraform plan -target=module.etl_table_combine
terraform apply -target=module.etl_table_combine
```

### 3. 验证部署结果
- AWS Glue控制台检查Job: `insightflow-table-combine-job`
- S3检查脚本上传: `s3://insightflow-dev-scripts/ETL/table_combine/ETL_table_combine.py`

## 运行Job

### 手动运行
在AWS Glue控制台点击"Run job"

### 验证输出
```bash
# 检查合并后的大表
aws s3 ls s3://insightflow-dev-clean-bucket/combined/
aws s3 ls s3://insightflow-dev-clean-bucket/combined/eval_set=prior/
aws s3 ls s3://insightflow-dev-clean-bucket/combined/eval_set=train/
```

## 预期输出结构

```
s3://insightflow-dev-clean-bucket/combined/
├── eval_set=prior/
│   ├── part-00000-xxx.parquet
│   └── part-00001-xxx.parquet
├── eval_set=train/
│   ├── part-00000-xxx.parquet
│   └── part-00001-xxx.parquet
└── eval_set=test/
    └── part-00000-xxx.parquet
```

## 大表字段说明

合并后的大表包含以下字段：
- **orders表字段**: order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order
- **order_products字段**: product_id, add_to_cart_order, reordered
- **products表字段**: product_name, aisle_id, department_id
- **aisles表字段**: aisle
- **departments表字段**: department

## 故障排除

### 常见问题

1. **Job运行失败 - 输入数据不存在**
   - 确保data_clean阶段已成功运行
   - 检查S3路径是否正确

2. **内存不足错误**
   - 增加worker数量: `etl_number_of_workers = 10`
   - 或使用更大的worker类型: `etl_worker_type = "G.2X"`

3. **权限错误**
   - 检查IAM角色权限
   - 确保对S3 bucket有读写权限

### 监控和日志

- **CloudWatch日志**: `/aws-glue/jobs/logs-v2`
- **Glue Job指标**: AWS Glue控制台的Monitoring标签
- **S3访问日志**: 检查bucket访问模式

## 性能优化建议

1. **分区策略**: 已按eval_set分区，适合后续按数据集类型过滤
2. **Join优化**: 小表使用broadcast join，减少shuffle操作
3. **文件大小**: 输出文件大小适中，避免小文件问题

## 下一步

table_combine模块成功运行后，可以进行各表的transformation:
- 每个表的转置脚本将从`s3://insightflow-dev-clean-bucket/combined/`读取大表
- 避免重复的join操作，提高整体ETL性能
