# Customer Behavior Analysis Dashboard

## Quick Setup

### 1. Download Data Files
```bash
# Install Git LFS
brew install git-lfs  # macOS
# or: sudo apt-get install git-lfs  # Ubuntu/Debian

# Download data files
git lfs install
git lfs pull

# Verify files exist (should show files > 100MB)
ls -la ../imba_data/
```

### 2. Configure Environment
```bash
# Copy and edit environment file
cp backend/env_template.txt .env

# Add your credentials to .env:
# OPENAI_API_KEY=your_openai_api_key
# AWS_ACCESS_KEY_ID=your_aws_access_key  
# AWS_SECRET_ACCESS_KEY=your_aws_secret_key
# AWS_DEFAULT_REGION=ap-southeast-2
```

### 3. Start Services
```bash
docker-compose up -d
```

### 4. Access Applications
- **Frontend**: http://localhost:8501
- **Backend API**: http://localhost:8000

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Frontend (Streamlit) | 8501 | Dashboard & chat interface |
| Backend (FastAPI) | 8000 | AI chat, ML models, vector search |
| AWS S3Vectors | - | Product embeddings storage |

## Key Commands

```bash
# Start/Stop
docker-compose up -d
docker-compose down

# View logs
docker-compose logs -f

# Rebuild
docker-compose up --build -d
```

## Data Requirements

The system expects these files in `../imba_data/`:
- `orders.csv` (~100MB)
- `products.csv` (~1MB) 
- `departments.csv` (~1KB)
- `aisles.csv` (~10KB)
- `order_products__prior.csv.gz` (~150MB)
- `order_products__train.csv.gz` (~7MB)

## Troubleshooting

### Common Issues
1. **"Not a gzipped file"**: Run `git lfs pull`
2. **Memory issues**: Ensure Docker has 4GB+ memory
3. **AWS errors**: Check credentials in `.env`
4. **Service not starting**: Check logs with `docker-compose logs`

### System Requirements
- **Memory**: 4GB+ recommended
- **Docker**: Latest version
- **Git LFS**: For data file download

## Documentation

- **[Backend](backend/README.md)** - FastAPI with AI chat & S3Vectors
- **[Frontend](frontend/README.md)** - Streamlit dashboard 