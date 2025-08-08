# ALS Model Upload/Download Scripts

This directory contains scripts to extract the ALS model from the Docker container and upload it to S3, as well as download it back.

## Prerequisites

1. **AWS Credentials**: Make sure your AWS credentials are set in environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID="your_access_key"
   export AWS_SECRET_ACCESS_KEY="your_secret_key"
   export AWS_DEFAULT_REGION="us-east-1"
   ```

2. **S3 Bucket**: Ensure the `S3_VECTORS_BUCKET` environment variable is set:
   ```bash
   export S3_VECTORS_BUCKET="your-s3-bucket-name"
   ```

3. **Docker Compose**: Make sure the containers are running:
   ```bash
   docker-compose up -d
   ```

## Scripts

### 1. Upload Model to S3

#### Option A: Simple Upload Script
```bash
python3 simple_model_upload.py
```

#### Option B: Advanced Upload Script
```bash
python3 extract_and_upload_model.py
```

**What it does:**
- Extracts the ALS model from `/app/als_model.pkl` in the container
- Uploads it to S3 with a timestamped filename
- Cleans up temporary files

**Output:**
```
🚀 Simple ALS Model Upload to S3
========================================
📥 Extracting model from container...
Running: docker cp insightflow-backend-1:/app/als_model.pkl temp_models/als_model.pkl
✅ Model extracted: temp_models/als_model.pkl (1564729 bytes)
📤 Uploading to S3...
✅ Model uploaded to: s3://your-bucket/models/als_model_20250807_143521.pkl
🧹 Cleaned up temporary file

📋 Summary:
• Model extracted from: insightflow-backend-1:/app/als_model.pkl
• Model uploaded to: s3://your-bucket/models/als_model_20250807_143521.pkl
• File size: 1564729 bytes
```

### 2. Download Model from S3

```bash
python3 download_model_from_s3.py
```

**What it does:**
- Lists all available ALS models in S3
- Downloads the most recent model
- Saves it locally as `als_model.pkl`

**Output:**
```
📥 ALS Model Download from S3
========================================
📋 Available models in S3:
1. models/als_model_20250807_143521.pkl
   Size: 1564729 bytes
   Last modified: 2025-08-07 14:35:21+00:00

📥 Using most recent model: models/als_model_20250807_143521.pkl
📥 Downloading models/als_model_20250807_143521.pkl to als_model.pkl...
✅ Model downloaded: als_model.pkl (1564729 bytes)

✅ Success! Model downloaded to: als_model.pkl
```

## Manual Commands

If you prefer to do it manually:

### Extract Model from Container
```bash
# Get container name
docker ps | grep backend

# Extract model (replace CONTAINER_NAME with actual container name)
docker cp CONTAINER_NAME:/app/als_model.pkl ./als_model.pkl
```

### Upload to S3 Manually
```bash
# Upload to S3
aws s3 cp als_model.pkl s3://your-bucket/models/als_model_$(date +%Y%m%d_%H%M%S).pkl
```

### Download from S3 Manually
```bash
# List available models
aws s3 ls s3://your-bucket/models/

# Download specific model
aws s3 cp s3://your-bucket/models/als_model_20250807_143521.pkl ./als_model.pkl
```

## Model Information

- **Location in container**: `/app/als_model.pkl`
- **Size**: ~1.5MB (varies based on data)
- **Contents**: Pickled ALS model with user/product mappings
- **S3 path**: `s3://your-bucket/models/als_model_TIMESTAMP.pkl`

## Troubleshooting

### Common Issues

1. **Container not found**: Make sure the container is running
   ```bash
   docker-compose ps
   ```

2. **AWS credentials not set**: Check environment variables
   ```bash
   echo $AWS_ACCESS_KEY_ID
   echo $S3_VECTORS_BUCKET
   ```

3. **Permission denied**: Make sure you have S3 write permissions

4. **Model file not found**: The model might be in a different location
   ```bash
   docker-compose exec backend find /app -name "*.pkl"
   ```

### Debug Commands

```bash
# Check if model exists in container
docker-compose exec backend ls -la /app/als_model.pkl

# Check file size
docker-compose exec backend stat /app/als_model.pkl

# List S3 bucket contents
aws s3 ls s3://your-bucket/models/
``` 