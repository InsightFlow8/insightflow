# WSL/Linux Scripts Usage Guide

这个目录包含了为 WSL/Linux 环境优化的脚本文件。

## 脚本文件说明

### 1. `create_s3_structure.sh`
**用途**: 为 Glue Crawlers 创建必要的 S3 目录结构
**执行前**: 确保 AWS CLI 已配置

```bash
# 赋予执行权限
chmod +x create_s3_structure.sh

# 使用默认参数
./create_s3_structure.sh

# 使用自定义参数
./create_s3_structure.sh your-bucket-name your/prefix
```

### 2. `validate_terraform.sh`
**用途**: 验证 Terraform 配置文件
**功能**: 初始化、验证、格式检查、计划预览

```bash
# 赋予执行权限
chmod +x validate_terraform.sh

# 运行验证
./validate_terraform.sh
```

### 3. `validate_terraform.ps1`
**用途**: PowerShell 版本的验证脚本（如果在 Windows 上直接运行 Terraform）

## 推荐工作流程

1. **配置 AWS 凭证**:
   ```bash
   aws configure
   ```

2. **创建 S3 结构**:
   ```bash
   ./create_s3_structure.sh
   ```

3. **验证 Terraform 配置**:
   ```bash
   ./validate_terraform.sh
   ```

4. **部署资源**:
   ```bash
   terraform apply
   ```

## 注意事项

- 🔧 **权限**: 确保脚本有执行权限 (`chmod +x script.sh`)
- 🔑 **AWS 凭证**: 确保 AWS CLI 已正确配置
- 📁 **工作目录**: 在 `terraform/dev/` 目录下执行脚本
- 🌐 **网络**: 确保 WSL 可以访问 AWS 服务

## 故障排除

### AWS CLI 相关
```bash
# 检查 AWS CLI 安装
aws --version

# 检查 AWS 凭证
aws sts get-caller-identity

# 重新配置 AWS
aws configure
```

### 权限问题
```bash
# 赋予所有脚本执行权限
chmod +x *.sh
```

### Terraform 相关
```bash
# 检查 Terraform 版本
terraform version

# 重新初始化
terraform init

# 格式化代码
terraform fmt -recursive
```
