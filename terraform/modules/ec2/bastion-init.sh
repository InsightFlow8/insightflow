#!/bin/bash

# =============================================================================
# Bastion Host Initialization Script
# =============================================================================
#
# PURPOSE:
#   This script initializes an EC2 bastion host for the InsightFlow project.
#   It installs Docker, Docker Compose, Git, clones the repository, downloads
#   required files from S3, and deploys the dashboard using Docker Compose.
#
# DEPLOYMENT LOCATION:
#   ⚠️  IMPORTANT: The insightflow project is deployed in the ROOT directory
#       (/root/insightflow/) NOT in the ec2-user home directory.
#
#   This happens because:
#   1. The script runs with elevated privileges (sudo)
#   2. The working directory during execution becomes /root/
#   3. Git clone creates the repository in the current working directory
#
#   📁 FOLDER STRUCTURE AFTER DEPLOYMENT:
#   /root/
#   └── insightflow/
#       ├── dashboard/
#       │   ├── docker-compose.yml      # Docker Compose configuration
#       │   ├── .env                    # Environment variables (from S3)
#       │   ├── als_model.pkl          # ML model (from S3)
#       │   ├── backend/               # Backend application code
#       │   ├── frontend/              # Frontend application code
#       │   └── data/                  # Data directory
#       ├── terraform/                 # Infrastructure as Code
#       ├── functions/                 # Lambda functions
#       └── ...                        # Other project files
#
# ACCESSING THE DASHBOARD:
#   To access the dashboard files and manage Docker containers:
#   ```bash
#   # Switch to root user
#   sudo su -
#   cd /root/insightflow/dashboard
#   
#   # Check Docker containers
#   docker-compose ps
#   
#   # View logs
#   docker-compose logs -f
#   
#   # Restart services
#   docker-compose restart
#   ```
#
# DOCKER SERVICES:
#   - Backend API: http://localhost:8000 (or EC2 public IP:8000)
#   - Frontend UI: http://localhost:8501 (or EC2 public IP:8501)
#   - Health Check: http://localhost:8000/health
#
# PREREQUISITES:
#   - EC2 instance with Amazon Linux 2023
#   - IAM role with S3 access permissions
#   - Internet connectivity for GitHub and Docker Hub
#   - Sufficient disk space (25GB+ recommended)
#
# LOGGING:
#   All script output is logged to /var/log/bastion-init.log
#
# ERROR HANDLING:
#   Script exits on first error with detailed logging
#   Each major step includes status checking and validation
#
# =============================================================================


set -ex

# Create log file
exec > >(tee /var/log/bastion-init.log) 2>&1
echo "=== Bastion Init Script Started at $(date) ==="

# Function to log messages with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if command succeeded
check_status() {
    if [ $? -eq 0 ]; then
        log_message "✅ $1 completed successfully"
    else
        log_message "❌ $1 failed"
        exit 1
    fi
}

# Install Docker on EC2
log_message "🚀 Installing Docker..."
sudo yum update -y
sudo dnf install -y docker
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -a -G docker ec2-user

# Fix socket permissions temporarily
sudo chmod 666 /var/run/docker.sock
check_status "Docker installation"

echo "Installing Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
fi
check_status "Docker Compose installation"

# Install Git and Git LFS
log_message "📦 Installing Git..."
sudo dnf install git -y 
check_status "Git installation"

# Clone the project from the specific branch
# NOTE: This creates the insightflow folder in the current working directory (/root/)
log_message "📥 Cloning project from GitHub..."
git clone -b main https://github.com/InsightFlow8/insightflow.git
check_status "Project cloning"

cd insightflow/dashboard

# Download environment file and ALS model
log_message "📁 Downloading environment file and ALS model..."
aws s3 cp s3://insightflow-dev-scripts-bucket/env/.env . 
check_status "Environment file download"

# Download the latest ALS model (with error handling)
log_message "🤖 Downloading ALS model from S3..."
rm -rf backend/als_model && mkdir -p backend/als_model
aws s3 cp s3://insightflow-dev-clean-bucket/ml/recsys/models/als-20250815-180121/als_model/ backend/als_model/

# Build and run the dashboard
log_message "🐳 Building and starting dashboard containers..."
docker-compose down 2>/dev/null || true  # Stop any existing containers
docker-compose build --no-cache  # Ensure fresh build
check_status "Docker build"

docker-compose up -d
check_status "Dashboard startup"

# Wait for services to be ready
log_message "⏳ Waiting for services to be ready..."
sleep 30

# Check if services are running
log_message "📊 Checking service status..."
docker-compose ps


# Configure SSH forwarding
log_message "🔐 Configuring SSH forwarding..."
echo "AllowTcpForwarding yes" | sudo tee -a /etc/ssh/sshd_config
echo "PermitTunnel yes" | sudo tee -a /etc/ssh/sshd_config
echo "AllowAgentForwarding yes" | sudo tee -a /etc/ssh/sshd_config
sudo systemctl restart sshd
check_status "SSH configuration"

# Final status
log_message "🎉 Dashboard deployment completed successfully!"
log_message "📋 Service URLs:"
log_message "   - Backend API: http://localhost:8000"
log_message "   - Frontend UI: http://localhost:8501"
log_message "   - Health Check: http://localhost:8000/health"
log_message "📁 Project location: /root/insightflow/ (NOT in /home/ec2-user/)"
log_message "🐳 Docker management: cd /root/insightflow/dashboard && docker-compose [command]"

log_message "=== Bastion Init Script Completed at $(date) ==="

