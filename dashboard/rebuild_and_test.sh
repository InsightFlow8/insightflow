#!/bin/bash

echo "🚀 Rebuilding Docker Container and Testing Model Loading"
echo "========================================================"

# Stop existing containers
echo "📦 Stopping existing containers..."
docker-compose down

# Remove old images to ensure fresh build
echo "🧹 Removing old images..."
docker-compose rm -f

# Rebuild the backend container
echo "🔨 Rebuilding backend container..."
docker-compose build --no-cache backend

# Start the backend container
echo "🚀 Starting backend container..."
docker-compose up -d backend

# Wait for container to be ready
echo "⏳ Waiting for container to be ready..."
sleep 10

# Check if container is running
echo "📊 Checking container status..."
docker-compose ps

# Test model loading
echo "🧪 Testing model loading..."
docker-compose exec backend python backend/test_model_loading.py

# Check application logs
echo "📋 Application logs:"
docker-compose logs --tail=20 backend

echo ""
echo "✅ Rebuild and test completed!"
echo ""
echo "To test the full application:"
echo "  docker-compose logs -f backend"
echo ""
echo "To test API endpoints:"
echo "  curl http://localhost:8000/health"
echo "  curl -X POST http://localhost:8000/chat -H 'Content-Type: application/json' -d '{\"query\": \"What should I buy?\", \"user_id\": \"1\"}'" 