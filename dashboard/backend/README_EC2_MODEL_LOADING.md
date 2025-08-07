# EC2 ALS Model Loading Workflow

This document describes how the ALS model is loaded in the EC2 environment, where the model is downloaded from S3 and the application skips training.

## Workflow Overview

1. **EC2 Instance Startup**: The bastion-init script downloads the model from S3
2. **Application Startup**: The ML model module loads the pre-trained model
3. **Skip Training**: If the model file exists, training is skipped entirely

## Model File Location

- **Expected path**: `/app/als_model.pkl` (in container)
- **Environment variable**: `ALS_MODEL_PATH=/app/als_model.pkl`
- **File size**: ~1.5MB (varies based on data)

## Bastion Init Script (EC2)

The bastion-init script downloads the model from S3:

```bash
# From terraform/modules/ec2/bastion-init.sh.tpl
aws s3 cp s3://insightflow-ml/models/als_model_20250808_005353.pkl .
echo "ALS model copied"
```

## Application Model Loading

The `ml_model.py` module handles model loading with the following logic:

### 1. Model File Verification
```python
def verify_model_file(model_path):
    """Verify that the model file exists and is valid"""
    # Checks file existence and size
    # Returns True if valid, False otherwise
```

### 2. Enhanced Model Loading
```python
def load_als_model():
    """Load ALS model from saved pickle file with enhanced error handling"""
    # Verifies model file integrity
    # Checks for all required components
    # Provides detailed error messages for EC2 environment
```

### 3. Smart Initialization
```python
def initialize_ml_model():
    """Initialize the ML model - load from S3-downloaded file or train if needed"""
    # First tries to load existing model
    # Only trains if model file is missing or invalid
    # Provides helpful logging for EC2 environment
```

## Expected Log Output

When the model is successfully loaded from S3:

```
🚀 Initializing ML model...
✅ ALS model pickle file found: /app/als_model.pkl (1564729 bytes)
📚 Loading pre-trained model (skipping training)
📚 Loading ALS model from: /app/als_model.pkl
✅ Model file verified: /app/als_model.pkl (1564729 bytes)
✅ Loaded ALS model successfully
📊 Model info: 18902 users, 34911 products
📊 Model factors: 50 dimensions
🔍 Analyzing ALS score ranges...
✅ ALS score range analysis completed
✅ ML model initialization completed successfully (loaded existing model)
```

## Error Handling

### Model File Not Found
```
❌ Model file not found: /app/als_model.pkl
💡 In EC2 environment, ensure the model is downloaded from S3 before starting the application
```

### Invalid Model File
```
❌ Model file is missing required components: ['model', 'user_id_map']
💡 The model file may be corrupted or incompatible
```

### Training Fallback
If the model file is missing or invalid, the system will:
1. Log a warning
2. Attempt to train a new model
3. Continue with limited functionality if training fails

## Testing

### Test Model Loading
```bash
# In the container
python backend/test_model_loading.py
```

Expected output:
```
🧪 Testing ALS Model Loading
========================================
📁 Model path: /app/als_model.pkl
📁 Current directory: /app
✅ Model file exists: /app/als_model.pkl (1564729 bytes)
✅ Model file verification passed
🚀 Testing model initialization...
✅ Model loading test completed successfully!
```

### Test in Docker Environment
```bash
# Test in running container
docker-compose exec backend python backend/test_model_loading.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ALS_MODEL_PATH` | `als_model.pkl` | Path to the ALS model file |
| `PYTHONPATH` | `/app` | Python path for imports |
| `PYTHONUNBUFFERED` | `1` | Unbuffered Python output |

## Troubleshooting

### Common Issues

1. **Model file not found**
   - Check if the S3 download completed successfully
   - Verify the file path in the bastion-init script
   - Check file permissions

2. **Model file corrupted**
   - Re-download from S3
   - Check S3 object integrity
   - Verify the model was uploaded correctly

3. **Training fallback**
   - Check if data files are available
   - Verify Python dependencies
   - Check disk space for training

### Debug Commands

```bash
# Check if model file exists
docker-compose exec backend ls -la /app/als_model.pkl

# Check file size
docker-compose exec backend stat /app/als_model.pkl

# Test model loading
docker-compose exec backend python backend/test_model_loading.py

# Check environment variables
docker-compose exec backend env | grep ALS
```

## Performance Benefits

- **Faster startup**: No training time required
- **Consistent models**: Same model across all instances
- **Resource efficient**: No CPU/memory usage for training
- **Reliable**: Pre-tested and validated model

## Security Considerations

- Model file contains sensitive user/product mappings
- Ensure proper file permissions
- Consider encrypting the model file in S3
- Rotate model files periodically for security 