# Dashboard Deployment Steps

This document outlines the deployment process for the InsightFlow dashboard on EC2.

## Current Deployment Process

### 1. EC2 Instance Setup (bastion-init.sh.tpl)

The deployment is handled by the `bastion-init.sh.tpl` script which:

1. **Installs Dependencies**
   - Docker and Docker Compose
   - Git and Git LFS
   - AWS CLI (assumed to be pre-installed)

2. **Downloads Project**
   - Clones from GitHub branch: `IF-5-Dashboard-S3VectorBucket-Athena`
   - Pulls Git LFS files

3. **Downloads Configuration**
   - Environment file from S3: `s3://insightflow-dev-scripts-bucket/env/.env`
   - ALS model from S3: `s3://insightflow-ml/models/als_model_20250808_005353.pkl`

4. **Builds and Starts Services**
   - Builds Docker containers with `--no-cache`
   - Starts services with `docker-compose up -d`

5. **Health Checks**
   - Tests model loading
   - Checks backend health (port 8000)
   - Checks frontend health (port 8501)

## Key Improvements Made

### Enhanced Error Handling
- ✅ Timestamped logging
- ✅ Status checking for each step
- ✅ Graceful failure handling

### Model Download Robustness
- ✅ Falls back to latest model if specific version not found
- ✅ Verifies model file integrity
- ✅ Reports model file size

### Service Validation
- ✅ Waits for services to be ready
- ✅ Tests model loading functionality
- ✅ Health checks for both backend and frontend

## Potential Issues and Solutions

### 1. Model File Issues

**Issue**: Model file not found or corrupted
```bash
# Check if model exists in S3
aws s3 ls s3://insightflow-ml/models/ | grep als_model

# Verify model file integrity
ls -lh als_model.pkl
```

**Solution**: Script now includes fallback to latest model and integrity verification

### 2. Docker Build Issues

**Issue**: Docker build fails due to cache or dependencies
```bash
# Clean build
docker-compose down
docker-compose build --no-cache
```

**Solution**: Script now uses `--no-cache` for fresh builds

### 3. Service Startup Issues

**Issue**: Services fail to start or are not ready
```bash
# Check service status
docker-compose ps

# Check logs
docker-compose logs backend
docker-compose logs frontend
```

**Solution**: Script includes 30-second wait and health checks

### 4. Environment File Issues

**Issue**: Environment variables not properly configured
```bash
# Check if .env file exists
ls -la .env

# Verify environment variables
docker-compose exec backend env | grep -E "(AWS|S3|OPENAI)"
```

**Solution**: Script verifies environment file download

## Testing the Deployment

### Local Testing
```bash
# Test model loading
docker-compose exec backend python backend/test_model_loading.py

# Test API endpoints
curl http://localhost:8000/health
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "What should I buy?", "user_id": "1"}'
```

### EC2 Testing
```bash
# Check deployment logs
sudo tail -f /var/log/bastion-init.log

# Test services
curl http://localhost:8000/health
curl http://localhost:8501/_stcore/health

# Check model loading
docker-compose exec backend python backend/test_model_loading.py
```

## Monitoring and Troubleshooting

### Log Files
- **Deployment logs**: `/var/log/bastion-init.log`
- **Docker logs**: `docker-compose logs -f`
- **Application logs**: `docker-compose logs backend`

### Health Checks
- **Backend**: `http://localhost:8000/health`
- **Frontend**: `http://localhost:8501/_stcore/health`
- **Model**: `docker-compose exec backend python backend/test_model_loading.py`

### Common Commands
```bash
# Restart services
docker-compose restart

# Rebuild and restart
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# Check resource usage
docker stats

# Access container shell
docker-compose exec backend bash
```

## Environment Variables

### Required Environment Variables
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_DEFAULT_REGION`: AWS region
- `S3_VECTORS_BUCKET`: S3 bucket for vectors
- `OPENAI_API_KEY`: OpenAI API key

### Model Configuration
- `ALS_MODEL_PATH`: Path to ALS model file (default: `/app/als_model.pkl`)

## Security Considerations

1. **Model File**: Contains sensitive user/product mappings
2. **Environment File**: Contains API keys and credentials
3. **SSH Configuration**: Enables forwarding for secure access
4. **Docker Permissions**: Temporary fix for socket permissions

## Performance Optimization

1. **Model Loading**: Pre-downloaded model for faster startup
2. **Docker Caching**: Fresh builds ensure latest code
3. **Health Checks**: Ensures services are ready before marking deployment complete
4. **Resource Monitoring**: Check `docker stats` for resource usage

## Rollback Strategy

If deployment fails:
1. Check logs: `sudo tail -f /var/log/bastion-init.log`
2. Identify the failing step
3. Fix the issue and re-run the script
4. Or manually execute the remaining steps

## Next Steps

1. **Automated Testing**: Add automated tests for the deployment
2. **Monitoring**: Set up monitoring and alerting
3. **Backup Strategy**: Implement backup for model files
4. **Security Hardening**: Review and improve security measures 