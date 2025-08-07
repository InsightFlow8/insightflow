#!/bin/bash

set -ex

# Create log file
exec > >(tee /var/log/bastion-init.log) 2>&1
echo "=== Bastion Init Script Started at $(date) ==="

# Function to log messages with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}



# Configure SSH forwarding
log_message "🔐 Configuring SSH forwarding..."
echo "AllowTcpForwarding yes" | sudo tee -a /etc/ssh/sshd_config
echo "PermitTunnel yes" | sudo tee -a /etc/ssh/sshd_config
echo "AllowAgentForwarding yes" | sudo tee -a /etc/ssh/sshd_config
sudo systemctl restart sshd
check_status "SSH configuration"


Optional: PostgreSQL setup (commented out)
log_message "🗄️ Setting up PostgreSQL..."
sudo dnf install -y postgresql15
aws s3 cp ${sql_s3_path} /tmp/create_tables.sql
export PGPASSWORD="${db_password}"
psql -h ${rds_host} -U ${db_username} -d ${db_name} -p ${rds_port} -f /tmp/create_tables.sql


