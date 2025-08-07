#!/bin/bash

set -ex

# Create log file
exec > >(tee /var/log/bastion-init.log) 2>&1
echo "=== Bastion Init Script Started at $(date) ==="

# Install Docker on EC2
echo "Installing Docker..."
sudo yum update -y
sudo dnf install -y docker
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -a -G docker ec2-user

# Fix socket permissions temporarily
sudo chmod 666 /var/run/docker.sock
echo "Docker installation completed"

echo "Installing Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
fi
echo "Docker Compose installation completed"


# Upload project to EC2
sudo dnf install git -y 
sudo dnf install -y git-lfs
git lfs install
echo "Git installation completed"

# Clone the project from the specific branch, IF-5-Dashboard-S3VectorBucket. Delete the branch after merging with main.
git clone -b IF-5-Dashboard-S3VectorBucket https://github.com/InsightFlow8/insightflow.git
echo "Project cloning completed"
cd insightflow/dashboard
git lfs pull
aws s3 cp s3://insightflow-dev-scripts-bucket/env/.env . 

# Build and run the dashboard
docker-compose up -d
echo "Dashboard startup completed"

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

