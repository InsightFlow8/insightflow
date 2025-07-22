#!/bin/bash

set -ex

# Forward SSH
echo "AllowTcpForwarding yes" | sudo tee -a /etc/ssh/sshd_config
echo "PermitTunnel yes" | sudo tee -a /etc/ssh/sshd_config
echo "AllowAgentForwarding yes" | sudo tee -a /etc/ssh/sshd_config
sudo systemctl restart sshd

# 安装 PostgreSQL 客户端
sudo dnf install -y postgresql15

# 拷贝 SQL 文件到本地 /tmp 目录
aws s3 cp ${sql_s3_path} /tmp/create_tables.sql

# 设置 PostgreSQL 密码环境变量，实现自动登录
export PGPASSWORD="${db_password}"
psql -h ${rds_host} -U ${db_username} -d ${db_name} -p ${rds_port} -f /tmp/create_tables.sql