# Summary
A dashboard with a comprehensive Customer Behavior Analysis Dashboard with AI-powered product recommendations, built using Streamlit frontend, FastAPI backend, AWS S3Vectors for vector storage, and AWS Athena for data analysis.

![](analysis.png)
![](chat_interface.png)

# Architecture Overview

## Core Components

### Frontend (Streamlit)
- **Port:** 8501
- **URL:** http://localhost:8501
- **Features:** Interactive dashboard with analytics, chat interface, and data visualization
- **Documentation:** [Frontend README](frontend/README.md)

### Backend (FastAPI)
- **Port:** 8000
- **URL:** http://localhost:8000
- **Features:** AI chat, ML models, vector search with S3Vectors, data analysis with Athena
- **Documentation:** [Backend README](backend/README.md)

### AWS S3 Vector Bucket
- **Service:** AWS S3Vectors (managed service)
- **Features:** Product embeddings storage and similarity search
- **Configuration:** Set via environment variables
- **Benefits:** Scalable, managed, cost-effective vector storage

### AWS Athena Integration
- **Service:** AWS Athena (serverless query service)
- **Purpose:** Data analysis and customer behavior insights
- **Features:** 
  - SQL queries on S3 data for customer segmentation
  - Purchase pattern analysis
  - Product affinity scoring
  - Customer lifetime value calculations
  - Churn analysis
- **Benefits:** Serverless, pay-per-query, no infrastructure management

# Docker Setup Guide

## Quick Start

1. **Create environment file:**
   ```bash
   # Copy template to dashboard/ folder (not backend/)
   cp env_template.txt .env
   # Edit .env and add your OPENAI_API_KEY and AWS credentials
   # Add your credentials to .env:
   # OPENAI_API_KEY=your_openai_api_key
   # AWS_ACCESS_KEY_ID=your_aws_access_key  
   # AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   # AWS_DEFAULT_REGION=ap-southeast-2
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

3. **Access the applications:**
   - Frontend: http://localhost:8501
   - Backend API: http://localhost:8000
   - AWS S3 Vector Bucket: Managed via AWS Console
   - AWS Athena: Managed via AWS Console

## Commands

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Rebuild and start
docker-compose up --build -d

# Access specific service
docker-compose exec backend python
docker-compose exec frontend streamlit run frontend/main.py
```

## Development

For development, you can mount volumes to see changes immediately:

```bash
# Start with volume mounts for development
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

# EC2 Deployment

## Prerequisites

1. **AWS Account Setup:**
   - AWS CLI configured with appropriate permissions
   - EC2 instance with sufficient resources (t2.medium or larger recommended)
   - Security groups configured for ports 80, 443, 8501, 8000
   - S3 bucket for model storage

2. **Required AWS Services:**
   - EC2 instance (t2.medium or larger)
   - S3 bucket for ALS model storage
   - IAM roles with appropriate permissions
   - VPC with internet connectivity

## Deployment Steps

### 1. Launch EC2 Instance

```bash
# Using AWS CLI
aws ec2 run-instances \
  --image-id ami-00839deb72faa8a04 \
  --instance-type t2.medium \
  --key-name your-key-pair \
  --security-group-ids sg-xxxxxxxxx \
  --subnet-id subnet-xxxxxxxxx \
  --user-data file://terraform/modules/ec2/bastion-init.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=insightflow-dashboard}]'
```

### 2. Configure Security Groups

Ensure the following ports are open:
- **Port 80/443**: HTTP/HTTPS (for web access)
- **Port 8501**: Streamlit frontend
- **Port 8000**: FastAPI backend
- **Port 22**: SSH (for management)

### 3. Access the Application

Once deployed, access the application at:
- **Frontend**: `http://your-ec2-public-ip:8501`
- **Backend API**: `http://your-ec2-public-ip:8000`

### 4. Monitoring and Logs

```bash
# SSH into the EC2 instance
ssh -i your-key.pem ec2-user@your-ec2-public-ip

# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Check specific service logs
docker-compose logs -f backend
docker-compose logs -f frontend

# Check system resources
htop
df -h

# Check bastion initialization logs
sudo cat /var/log/bastion-init.log

# Follow bastion logs in real-time
sudo tail -f /var/log/bastion-init.log

# Check Docker container logs
docker-compose logs -f

# Check specific container logs
docker-compose logs -f backend-1
docker-compose logs -f frontend-1
```

## EC2 Configuration Details

### Instance Specifications
- **Instance Type**: t2.medium (2 vCPU, 4GB RAM) - **Recommended: t3.large for better performance**
- **Storage**: 25GB GP3 EBS volume
- **OS**: Amazon Linux 2
- **Architecture**: x86_64

### Automated Setup
The deployment uses a `bastion-init.sh` script that automatically:
1. Updates system packages
2. Installs Docker and Docker Compose
3. Installs Git
4. Downloads the ALS model from S3
5. Clones the repository
6. Builds and starts the Docker containers
7. Configures health checks

### Environment Variables for EC2

Create a `.env` file on the EC2 instance:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=ap-southeast-2

# S3 Configuration
S3_VECTORS_BUCKET=your-vector-bucket
S3_VECTORS_INDEX=products-index
S3_MODEL_BUCKET=your-model-bucket

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key

# Athena Configuration
ATHENA_DATABASE=your_athena_database
ATHENA_WORKGROUP=primary
ATHENA_OUTPUT_BUCKET=your-athena-output-bucket
```

## Data Configuration

### AWS S3 Data Storage
The system uses AWS S3 for data storage and AWS Athena for querying. Data is stored in S3 buckets and accessed through Athena queries.

### Expected File Structure
After downloading, `imba_data/` should contain:
```
imba_data/
├── orders.csv            
├── products.csv                  # ~1MB
├── departments.csv               # ~1KB
├── aisles.csv                    # ~10KB
├── order_products__prior.csv.gz  # ~150MB (compressed)
└── order_products__train.csv.gz  # ~7MB (compressed)
```

### Data Filtering for Testing
For efficient testing, the data loaders automatically filter the data:
- **User ID restriction**: Only users with `user_id < 10000` are loaded
- **Product restriction**: Only products with `product_id < 1000` are used for vector store
- This significantly reduces memory usage and processing time

### Memory Configuration
The docker-compose.yml specifies memory limits for each service:
- **Backend**: 4GB limit, 2GB reservation
- **Frontend**: 4GB limit, 2GB reservation

**For lower memory systems**, you can reduce these limits:
```yaml
# In docker-compose.yml, modify the deploy section:
deploy:
  resources:
    limits:
      memory: 2G  # Reduce from 4G
    reservations:
      memory: 1G  # Reduce from 2G
```

## AWS S3 Vector Bucket Configuration

### Environment Variables
The system uses AWS S3Vectors for vector storage. Configure these in your `.env` file in the `dashboard/` folder:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=ap-southeast-2

# S3 Vector Bucket Configuration
S3_VECTORS_BUCKET=imba-vector-database
S3_VECTORS_INDEX=products-index

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key
```

### Benefits of S3Vectors
- ✅ **Managed Service**: No infrastructure management required
- ✅ **Scalable**: Handles large vector datasets efficiently
- ✅ **Cost-effective**: Pay only for what you use
- ✅ **High Performance**: Optimized for vector similarity search
- ✅ **Metadata Support**: Rich metadata for product information

## Troubleshooting

### Data Issues
1. **"Not a gzipped file" error**: Data files are still Git LFS pointers
   ```bash
   git lfs pull  # Download actual files
   ```

2. **"File not found" error**: Check data files exist
   ```bash
   ls -la ../imba_data/
   ```

3. **Memory issues**: Ensure Docker has sufficient memory (4GB+ recommended)

### Service Issues
1. **Check service health:**
   ```bash
   docker-compose ps
   ```

2. **View logs:**
   ```bash
   docker-compose logs backend
   docker-compose logs frontend
   ```

3. **Restart specific service:**
   ```bash
   docker-compose restart backend
   ```

4. **Rebuild containers:**
   ```bash
   docker-compose down
   docker-compose up --build -d
   ```

### AWS S3Vectors Issues
1. **Authentication errors**: Check AWS credentials in `.env` file in `dashboard/` folder
2. **Bucket not found**: Verify S3_VECTORS_BUCKET exists in AWS
3. **Network connectivity**: Ensure backend can reach AWS services

### EC2 Deployment Issues
1. **Check bastion script logs:**
   ```bash
   # View complete bastion initialization log
   sudo cat /var/log/bastion-init.log
   
   # Follow logs in real-time during deployment
   sudo tail -f /var/log/bastion-init.log
   
   # Check for specific errors
   sudo grep -i "error\|failed\|❌" /var/log/bastion-init.log
   ```

2. **Check Docker container status:**
   ```bash
   # Navigate to dashboard directory
   cd /root/insightflow/dashboard
   
   # Check container status
   docker-compose ps
   
   # View all container logs
   docker-compose logs -f
   
   # Check specific service logs
   docker-compose logs -f backend
   docker-compose logs -f frontend
   ```

3. **Verify system resources:**
   ```bash
   # Check available memory
   free -h
   
   # Check disk space
   df -h
   
   # Check running processes
   ps aux | grep docker
   ```

## Documentation

For detailed information about each component:

- **[Backend Documentation](backend/README.md)** - FastAPI backend with AI chat, ML models, and S3Vectors search
- **[Frontend Documentation](frontend/README.md)** - Streamlit dashboard with analytics and chat interface
- **[Frontend Structure Guide](frontend/README_STRUCTURE.md)** - Detailed breakdown of frontend components and architecture

## Amazon S3 Vectors Pricing Breakdown

1. Data Upload / Ingestion
	-	$0.20 per GB ingested into S3 vector indexes via PutVectors  ￼

2. Storage
	-	$0.06 per GB per month for storing vector data in S3 Vectors  ￼

3. Query Processing
	-	Charged based on number of queries and data scanned.
	-	For example (from AWS case study):
	-	With 400 million vectors, 40 indexes, 10 million queries:
	-	~$996.62 in query cost,
	-	$78.46 ingest,
	-	$141.22 storage per month

#### Example Use Case Cost Estimate
Suppose you store 60 GB of vector data (e.g. 10 million vectors of ~1,536‑dim) and run 1 million similarity queries per month:

| **Component** | **Usage**         | **Unit Price**    | **Total Cost**    |
| ------------- | ----------------- | ----------------- | ----------------- |
| **Ingest**    | 60 GB             | $0.20 / GB        | **$12.00**        |
| **Storage**   | 60 GB (per month) | $0.06 / GB        | **$3.60 / month** |
| **Queries**   | 1,000,000 queries | ≈ $0.004 / 1K req | **$4.00**         |