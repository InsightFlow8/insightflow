# Docker Setup Guide

## Quick Start

1. **Set up data files (IMPORTANT):**
   ```bash
   # Install Git LFS if not already installed
   brew install git-lfs  # macOS
   # or: sudo apt-get install git-lfs  # Ubuntu/Debian
   
   # Initialize Git LFS and download data files
   git lfs install
   git lfs pull
   
   # Verify data files are downloaded (should show files > 100MB)
   ls -la ../imba_data/
   ```

2. **Create environment file:**
   ```bash
   cp backend/env_template.txt .env
   # Edit .env and add your OPENAI_API_KEY
   ```

3. **Start all services:**
   ```bash
   docker-compose up -d
   ```

4. **Access the applications:**
   - Frontend: http://localhost:8501
   - Backend API: http://localhost:8000
   - Qdrant: http://localhost:6333

## Services

### Frontend (Streamlit)
- **Port:** 8501
- **URL:** http://localhost:8501
- **Features:** Dashboard with analytics and chat interface

### Backend (FastAPI)
- **Port:** 8000
- **URL:** http://localhost:8000
- **Features:** AI chat, ML models, vector search

### Qdrant (Vector Database)
- **Port:** 6333
- **URL:** http://localhost:6333
- **Features:** Product embeddings storage

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

## Data Setup

### Git LFS Files
The data files in `imba_data/` are stored using Git LFS (Large File Storage). You must download the actual data files before running the dashboard.

### Download Data Files
```bash
# Install Git LFS
brew install git-lfs  # macOS
# or: sudo apt-get install git-lfs  # Ubuntu/Debian

# Initialize and download
git lfs install
git lfs pull

# Verify download (files should be > 100MB)
ls -la ../imba_data/
```

### Expected File Structure
After downloading, `imba_data/` should contain:
```
imba_data/
├── orders.csv                    # ~100MB
├── products.csv                  # ~1MB
├── departments.csv               # ~1KB
├── aisles.csv                    # ~10KB
├── order_products__prior.csv.gz  # ~150MB (compressed)
└── order_products__train.csv.gz  # ~7MB (compressed)
```

### Alternative: Sample Data
For testing without large files, create sample data:
```python
import pandas as pd

# Create sample orders
sample_orders = pd.DataFrame({
    'order_id': range(1, 101),
    'user_id': range(1, 21) * 5,
    'order_number': range(1, 101),
    'order_dow': [1, 2, 3, 4, 5] * 20,
    'order_hour_of_day': [9, 10, 11, 12, 13] * 20,
    'days_since_prior_order': [7, 14, 21, 28, 35] * 20
})

# Save to imba_data directory
sample_orders.to_csv('../imba_data/orders.csv', index=False)
```

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