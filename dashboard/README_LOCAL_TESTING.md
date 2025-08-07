# Local Testing Guide for EC2 Model Loading

This guide shows how to test the EC2 model loading workflow locally using Docker.

## Prerequisites

1. **ALS Model File**: Ensure `als_model.pkl` is in the dashboard folder
2. **Docker Compose**: Make sure Docker is running
3. **Environment**: Set up environment variables if needed

## Step-by-Step Testing

### 1. Verify Model File

```bash
# Check if model file exists
ls -la als_model.pkl

# Expected output:
# -rw-r--r--@ 1 cy  staff  22547562 Aug  8 00:53 als_model.pkl
```

### 2. Start the Backend Container

```bash
# Start only the backend service
docker-compose up -d backend

# Check if container is running
docker-compose ps
```

### 3. Test Model Loading

```bash
# Test the model loading functionality
docker-compose exec backend python backend/test_model_loading.py
```

**Expected Output:**
```
🧪 Testing ALS Model Loading
========================================
📁 Model path: /app/als_model.pkl
📁 Current directory: /app
✅ Model file exists: /app/als_model.pkl (22547562 bytes)
✅ Model file verification passed
🚀 Testing model initialization...
✅ Model loading test completed successfully!
```

### 4. Test Full Application Startup

```bash
# Check the application logs
docker-compose logs backend

# Or follow logs in real-time
docker-compose logs -f backend
```

**Expected Log Output:**
```
🚀 Initializing ML model...
✅ ALS model pickle file found: /app/als_model.pkl (22547562 bytes)
📚 Loading pre-trained model (skipping training)
📚 Loading ALS model from: /app/als_model.pkl
✅ Model file verified: /app/als_model.pkl (22547562 bytes)
✅ Loaded ALS model successfully
📊 Model info: 18902 users, 34911 products
📊 Model factors: 50 dimensions
🔍 Analyzing ALS score ranges...
✅ ALS score range analysis completed
✅ ML model initialization completed successfully (loaded existing model)
```

### 5. Test API Endpoints

```bash
# Test the health endpoint
curl http://localhost:8000/health

# Test a recommendation endpoint
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "What should I buy?", "user_id": "1"}'
```

## Alternative Testing Methods

### Method 1: Direct Container Access

```bash
# Access the container directly
docker-compose exec backend bash

# Inside the container, test model loading
python backend/test_model_loading.py

# Check environment variables
env | grep ALS

# Check model file
ls -la /app/als_model.pkl
```

### Method 2: Test with Python Script

```bash
# Run the test script directly
docker-compose exec backend python -c "
from ml_model import initialize_ml_model, verify_model_file
import os

print('Testing model loading...')
model_path = os.getenv('ALS_MODEL_PATH', 'als_model.pkl')
print(f'Model path: {model_path}')

if verify_model_file(model_path):
    print('Model file verified successfully')
    initialize_ml_model()
    print('Model initialization completed')
else:
    print('Model file verification failed')
"
```

## Troubleshooting

### Common Issues

1. **Model file not found in container**
   ```bash
   # Check if file is mounted correctly
   docker-compose exec backend ls -la /app/als_model.pkl
   
   # If not found, check docker-compose.yml volumes
   cat docker-compose.yml | grep -A 5 volumes
   ```

2. **Permission issues**
   ```bash
   # Check file permissions
   docker-compose exec backend stat /app/als_model.pkl
   
   # Fix permissions if needed
   chmod 644 als_model.pkl
   ```

3. **Model file corrupted**
   ```bash
   # Check file integrity
   docker-compose exec backend python -c "
   import pickle
   try:
       with open('/app/als_model.pkl', 'rb') as f:
           data = pickle.load(f)
       print('Model file is valid')
       print(f'Keys: {list(data.keys())}')
   except Exception as e:
       print(f'Model file is corrupted: {e}')
   "
   ```

### Debug Commands

```bash
# Check container status
docker-compose ps

# Check container logs
docker-compose logs backend

# Check environment variables
docker-compose exec backend env | grep ALS

# Check file system
docker-compose exec backend ls -la /app/

# Check Python path
docker-compose exec backend python -c "import sys; print(sys.path)"
```

## Expected Results

### Successful Test

- ✅ Model file found and verified
- ✅ Model loaded successfully
- ✅ No training performed (skipped)
- ✅ Application starts quickly
- ✅ All ML functions work (recommendations, scores, etc.)

### Failed Test

- ❌ Model file not found
- ❌ Model file corrupted
- ❌ Training fallback triggered
- ❌ Application starts slowly (due to training)

## Performance Comparison

### With Pre-trained Model (EC2-like)
- **Startup time**: ~5-10 seconds
- **Memory usage**: Lower (no training data)
- **CPU usage**: Minimal (just loading)

### Without Pre-trained Model (Training)
- **Startup time**: ~30-60 seconds
- **Memory usage**: Higher (training data)
- **CPU usage**: High (training process)

## Next Steps

1. **Test API endpoints** with the loaded model
2. **Verify recommendations** work correctly
3. **Check score normalization** is working
4. **Test error handling** by removing the model file
5. **Deploy to EC2** and test the full workflow 