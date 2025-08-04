# Backend Setup Guide

## Environment Configuration

### 1. Create .env File

Create a `.env` file in the `backend/` directory with the following variables:

```bash
# Copy the template and fill in your values
cp env_template.txt .env
```

### 2. Required Environment Variables

```bash
# OpenAI API Configuration (REQUIRED)
OPENAI_API_KEY=your_openai_api_key_here

# Vector Store Configuration
QDRANT_HOST=qdrant
QDRANT_PORT=6333

# Server Configuration
HOST=0.0.0.0
PORT=8000

# Logging Configuration
LOG_LEVEL=INFO

# Model Configuration
ALS_MODEL_PATH=als_model.pkl
VECTOR_STORE_COLLECTION=products_collection
```

### 3. Get OpenAI API Key

1. Go to [OpenAI Platform](https://platform.openai.com/)
2. Create an account or sign in
3. Navigate to API Keys section
4. Create a new API key
5. Copy the key and paste it in your `.env` file

### 4. Verify Setup

```bash
# Check if .env file exists
ls -la backend/.env

# Verify environment variables are loaded
python -c "import os; print('OPENAI_API_KEY:', 'SET' if os.getenv('OPENAI_API_KEY') else 'NOT SET')"
```

## Security Best Practices

### ✅ Do:
- Keep `.env` file in `.gitignore`
- Use strong, unique API keys
- Rotate API keys regularly
- Use environment-specific configs

### ❌ Don't:
- Commit `.env` files to version control
- Share API keys publicly
- Use the same key across environments
- Hardcode secrets in code

## Troubleshooting

### Common Issues:

1. **"OPENAI_API_KEY not found"**
   - Check if `.env` file exists in `backend/` directory
   - Verify the API key is correctly set

2. **"Connection refused"**
   - Ensure Qdrant service is running
   - Check QDRANT_HOST and QDRANT_PORT settings

3. **"Model not found"**
   - The system will automatically train the model on first run
   - Check ALS_MODEL_PATH setting

## Development vs Production

### Development:
```bash
# Use local settings
HOST=localhost
PORT=8000
LOG_LEVEL=DEBUG
```

### Production:
```bash
# Use production settings
HOST=0.0.0.0
PORT=8000
LOG_LEVEL=INFO
``` 