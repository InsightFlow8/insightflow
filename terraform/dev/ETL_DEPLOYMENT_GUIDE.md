# ETL Data Clean Module Deployment Guide

## 脚本部署方式选择

### 方式1: 手动上传 (适合开发测试)
```bash
# 上传ETL脚本到S3
aws s3 cp functions/modules/ETL/data_clean/ETL_data_clean.py s3://insightflow-imba-scripts-upload/ETL/data_clean/
```

### 方式2: Terraform 自动上传 (推荐用于生产)
在 `terraform/modules/ETL/data_clean/` 中添加：
```hcl
resource "aws_s3_object" "etl_script" {
  bucket = var.scripts_bucket
  key    = "ETL/data_clean/ETL_data_clean.py"
  source = "../../functions/modules/ETL/data_clean/ETL_data_clean.py"
  etag   = filemd5("../../functions/modules/ETL/data_clean/ETL_data_clean.py")
}
```

### 方式3: GitHub Actions CI/CD (推荐用于团队协作)
自动检测代码变更并部署，详见 `.github/workflows/deploy-etl.yml.example`

## 业界最佳实践
- **开发阶段**: 手动上传，快速迭代
- **测试阶段**: Terraform 集成，确保环境一致性  
- **生产阶段**: CI/CD 自动化，版本控制和审核

## 前置条件

1. 确保已部署基础设施（S3 buckets, VPC等）
2. 选择合适的脚本部署方式
3. 确保原始数据已存在于 raw bucket 的对应路径

## 部署步骤

### 1. 上传ETL脚本
```bash
# 上传ETL脚本到S3
aws s3 cp functions/modules/ETL/data_clean/ETL_data_clean.py s3://insightflow-dev-scripts/ETL/data_clean/
```

### 2. 部署Terraform
```bash
cd terraform/dev
terraform init
terraform plan
terraform apply
```

### 3. 验证部署
```bash
# 检查Glue Job是否创建成功
aws glue get-job --job-name insightflow-data-clean-job

# 检查IAM Role
aws iam get-role --role-name insightflow-glue-data-clean-role
```

## 运行ETL Job

### 手动运行
```bash
aws glue start-job-run --job-name insightflow-data-clean-job
```

### 检查运行状态
```bash
# 获取最新运行ID
RUN_ID=$(aws glue get-job-runs --job-name insightflow-data-clean-job --query 'JobRuns[0].Id' --output text)

# 检查运行状态
aws glue get-job-run --job-name insightflow-data-clean-job --run-id $RUN_ID
```

### 查看日志
CloudWatch Logs Group: `/aws-glue/jobs/logs-v2`

## 输入输出路径

### 输入路径 (Raw Data)
- Aisles: `s3://insightflow-dev-raw-bucket/data/batch/aisles/`
- Departments: `s3://insightflow-dev-raw-bucket/data/batch/departments/`
- Products: `s3://insightflow-dev-raw-bucket/data/batch/products/`
- Orders: `s3://insightflow-dev-raw-bucket/data/batch/orders/`
- Order Products Prior: `s3://insightflow-dev-raw-bucket/data/batch/order_products_prior/`
- Order Products Train: `s3://insightflow-dev-raw-bucket/data/batch/order_products_train/`

### 输出路径 (Clean Data)
- Aisles: `s3://insightflow-dev-clean-bucket/temp/aisles/`
- Departments: `s3://insightflow-dev-clean-bucket/temp/departments/`
- Products: `s3://insightflow-dev-clean-bucket/temp/products/`
- Orders: `s3://insightflow-dev-clean-bucket/temp/orders/`
- Order Products Prior: `s3://insightflow-dev-clean-bucket/temp/order_products_prior/`
- Order Products Train: `s3://insightflow-dev-clean-bucket/temp/order_products_train/`

## 故障排除

### 常见问题
1. **权限不足**: 检查IAM Role是否有S3和CloudWatch权限
2. **脚本路径错误**: 确认ETL脚本已正确上传到S3
3. **输入数据不存在**: 确认原始数据存在于指定的S3路径

### 调试命令
```bash
# 检查S3中的输入数据
aws s3 ls s3://insightflow-dev-raw-bucket/data/batch/ --recursive

# 检查脚本是否存在
aws s3 ls s3://insightflow-dev-scripts/ETL_data_clean.py

# 查看最近的作业运行
aws glue get-job-runs --job-name insightflow-data-clean-job --max-items 5
```

## 配置修改

如需修改配置（如增加worker数量、更改路径等），请：
1. 更新 `terraform.tfvars` 中的相应值
2. 运行 `terraform plan` 确认更改
3. 运行 `terraform apply` 应用更改
