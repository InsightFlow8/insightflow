# Lambda S3 to RDS Sync Module

本模块用于自动化部署 Lambda 函数，实现 S3 → RDS 数据同步，支持 S3 事件、EventBridge 定时、手动触发。

## 主要资源
- Lambda 函数（Python，打包 zip）
- IAM 角色与策略
- 手动 & EventBridge 触发器，S3触发器暂时被注释掉了

## 变量说明
详见 variables.tf，需传递 S3/RDS 参数、打包 zip 路径、表名、批量大小等。

## 输出
详见 outputs.tf，包含 Lambda ARN、名称、EventBridge 规则 ARN。
