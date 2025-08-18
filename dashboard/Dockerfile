# Dockerfile
FROM python:3.11-bullseye

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -d /usr/lib/jvm/java-17-openjdk-amd64 ]; then \
        ln -s /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk; \
    elif [ -d /usr/lib/jvm/java-17-openjdk-arm64 ]; then \
        ln -s /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/java-17-openjdk; \
    fi

# Copy requirements first for better caching
COPY dashboard/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY dashboard/ .
COPY dashboard/backend/als_model/ /app/backend/als_model/

# Copy imba_data directory
COPY imba_data/ /app/imba_data/

# Create necessary directories
RUN mkdir -p /app/data /app/backend /app/frontend 

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV ALS_MODEL_PATH=/app/backend/als_model
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

# Expose ports
EXPOSE 8000 8501

# Default command (can be overridden in docker-compose)
CMD ["python", "backend/main.py"]