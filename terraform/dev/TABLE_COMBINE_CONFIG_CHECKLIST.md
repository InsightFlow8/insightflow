# ETL Table Combine 配置验证清单

## 配置完整性检查

### ✅ 已完成的配置

1. **Terraform模块文件**
   - ✅ `terraform/modules/ETL/table_combine/main.tf`
   - ✅ `terraform/modules/ETL/table_combine/iam.tf`  
   - ✅ `terraform/modules/ETL/table_combine/variables.tf`
   - ✅ `terraform/modules/ETL/table_combine/outputs.tf`
   - ✅ `terraform/modules/ETL/table_combine/script_upload.tf`
   - ✅ `terraform/modules/ETL/table_combine/README.md`

2. **PySpark脚本**
   - ✅ `functions/modules/ETL/table_combine/ETL_table_combine.py`
   - ✅ 完全参数化路径
   - ✅ 使用parquet格式读取
   - ✅ 按eval_set分区输出

3. **Dev层配置**
   - ✅ `terraform/dev/main.tf` - 已添加table_combine模块调用
   - ✅ `terraform/dev/variables.tf` - 已添加相关变量
   - ✅ `terraform/dev/terraform.tfvars` - 已添加配置值
   - ✅ `terraform/dev/outputs.tf` - 已添加输出定义

## 配置参数对应关系

### PySpark脚本参数 ↔ Terraform变量

| PySpark参数 | Terraform变量 | terraform.tfvars值 |
|-------------|---------------|-------------------|
| `aisles_path` | `var.aisles_output_path` | `s3://insightflow-dev-clean-bucket/temp/aisles/` |
| `departments_path` | `var.departments_output_path` | `s3://insightflow-dev-clean-bucket/temp/departments/` |
| `products_path` | `var.products_output_path` | `s3://insightflow-dev-clean-bucket/temp/products/` |
| `orders_path` | `var.orders_output_path` | `s3://insightflow-dev-clean-bucket/temp/orders/` |
| `order_products_prior_path` | `var.order_products_prior_output_path` | `s3://insightflow-dev-clean-bucket/temp/order_products_prior/` |
| `order_products_train_path` | `var.order_products_train_output_path` | `s3://insightflow-dev-clean-bucket/temp/order_products_train/` |
| `output_path` | `var.table_combine_output_path` | `s3://insightflow-dev-clean-bucket/combined/` |

### Glue Job配置

| 配置项 | 变量 | 值 |
|--------|------|------|
| Job名称 | `table_combine_job_name` | `insightflow-table-combine-job` |
| IAM角色 | `table_combine_iam_role_name` | `insightflow-glue-table-combine-role` |
| Glue版本 | `etl_glue_version` | `4.0` |
| Workers数量 | `etl_number_of_workers` | `5` |
| Worker类型 | `etl_worker_type` | `G.1X` |
| 临时目录 | `etl_temp_dir` | `s3://insightflow-dev-clean-bucket/glue-temp/` |

## 部署依赖关系

```
module.s3_buckets 
    ↓
module.etl_data_clean (必须先运行成功)
    ↓  
module.etl_table_combine
```

## 数据流向

```
Raw Data (S3) 
    ↓ (data_clean)
Cleaned Tables (s3://.../temp/*/) 
    ↓ (table_combine)
Combined Table (s3://.../combined/eval_set=*)
    ↓ (future transformations)
Transformed Tables
```

## 验证步骤

1. **部署前验证**
   ```bash
   # 检查前置数据
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/aisles/
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/departments/
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/products/
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/orders/
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/order_products_prior/
   aws s3 ls s3://insightflow-dev-clean-bucket/temp/order_products_train/
   ```

2. **部署执行**
   ```bash
   cd terraform/dev
   terraform plan -target=module.etl_table_combine
   terraform apply -target=module.etl_table_combine
   ```

3. **部署后验证**
   ```bash
   # 检查Glue Job
   aws glue get-job --job-name insightflow-table-combine-job
   
   # 检查脚本上传
   aws s3 ls s3://insightflow-dev-scripts/ETL/table_combine/
   
   # 运行Job后检查输出
   aws s3 ls s3://insightflow-dev-clean-bucket/combined/
   ```

## 故障排除

### 常见问题
1. **依赖模块未部署** → 先部署etl_data_clean模块
2. **输入数据不存在** → 运行data_clean阶段Job
3. **权限问题** → 检查IAM角色配置
4. **资源冲突** → 检查Job名称是否唯一

### 监控位置
- **CloudWatch日志**: `/aws-glue/jobs/logs-v2`
- **Glue控制台**: AWS Glue → ETL jobs → insightflow-table-combine-job
- **S3输出**: `s3://insightflow-dev-clean-bucket/combined/`

## 配置完整性确认 ✅

所有必要的配置文件已创建并正确关联，可以进行部署测试。
