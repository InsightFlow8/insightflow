# 🔧 测试验证故障排除指南

## 可能遇到的问题和解决方案

### 1. S3 对象创建失败
**错误**: `Error creating S3 object`
**原因**: S3 bucket 权限或不存在
**解决**:
```bash
# 检查 bucket 是否存在
aws s3 ls s3://insightflow-raw-bucket/

# 检查 AWS 凭证
aws sts get-caller-identity

# 重新创建 bucket（如果需要）
terraform apply -target=module.s3_buckets
```

### 2. Glue Crawler 创建失败
**错误**: `InvalidInputException: Unable to validate existence of s3 target`
**原因**: S3 对象依赖未正确设置
**解决**:
```bash
# 检查 S3 对象是否存在
aws s3 ls s3://insightflow-raw-bucket/data/batch/ --recursive

# 重新应用 Terraform
terraform refresh
terraform apply
```

### 3. IAM 权限问题
**错误**: `AccessDenied` 或 `InvalidAccessKeyId`
**原因**: IAM 角色权限不足
**解决**:
```bash
# 检查 IAM 角色是否存在
aws iam get-role --role-name insightflow_dev-glue-crawler-raw-role

# 检查策略是否附加
aws iam list-attached-role-policies --role-name insightflow_dev-glue-crawler-raw-role
```

### 4. 区域配置问题
**错误**: 资源在错误的区域创建
**解决**:
```bash
# 确认当前配置的区域
aws configure get region

# 检查 terraform.tfvars 中的区域设置
grep aws_region terraform.tfvars
```

## 验证检查清单

### 部署前检查 ✓
- [ ] AWS 凭证配置正确
- [ ] S3 bucket 为空但存在
- [ ] 没有现有的 Glue Crawler 资源
- [ ] Terraform 状态干净

### 部署后验证 ✓
- [ ] 6 个 S3 placeholder 文件创建
- [ ] 4 个 Glue Crawler 创建成功
- [ ] 1 个 Glue Database 创建
- [ ] IAM 角色和策略创建

### 功能测试 ✓
- [ ] 可以手动运行 Crawler
- [ ] Crawler 能够发现 placeholder 文件
- [ ] 没有权限错误

## 成功标准

部署成功的标志：
1. `terraform apply` 完成无错误
2. 所有预期资源在 AWS 控制台中可见
3. Glue Crawlers 状态为 "READY"
4. S3 目录结构完整创建
