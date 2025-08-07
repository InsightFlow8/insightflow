#!/usr/bin/env python3
"""
Script to extract ALS model from container and upload to S3
"""

import os
import sys
import boto3
import logging
from datetime import datetime
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_model_from_container():
    """Extract the ALS model file from the Docker container"""
    try:
        logger.info("🔍 Extracting ALS model from container...")
        
        # Create a temporary directory for the model
        os.makedirs("temp_models", exist_ok=True)
        
        # Copy the model file from the container
        container_path = "/app/als_model.pkl"
        local_path = "temp_models/als_model.pkl"
        
        # Use docker cp to extract the file
        cmd = f"docker-compose exec -T backend cat {container_path} > {local_path}"
        logger.info(f"Running: {cmd}")
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Failed to extract model: {result.stderr}")
            return None
        
        # Check if file was created and has content
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            file_size = os.path.getsize(local_path)
            logger.info(f"✅ Model extracted successfully: {local_path} ({file_size} bytes)")
            return local_path
        else:
            logger.error("❌ Model file is empty or was not created")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error extracting model: {e}")
        return None

def upload_model_to_s3(local_path, bucket_name=None, s3_key=None):
    """Upload the model file to S3"""
    try:
        # Get S3 configuration from environment
        bucket_name = bucket_name or os.getenv("S3_VECTORS_BUCKET")
        if not bucket_name:
            logger.error("❌ S3_VECTORS_BUCKET environment variable not set")
            return False
        
        # Generate S3 key if not provided
        if not s3_key:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"models/als_model_{timestamp}.pkl"
        
        logger.info(f"📤 Uploading model to S3: s3://{bucket_name}/{s3_key}")
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        # Upload the file
        s3_client.upload_file(
            local_path,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )
        
        logger.info(f"✅ Model uploaded successfully to s3://{bucket_name}/{s3_key}")
        
        # Get the S3 URL
        s3_url = f"s3://{bucket_name}/{s3_key}"
        logger.info(f"🔗 S3 URL: {s3_url}")
        
        return s3_url
        
    except Exception as e:
        logger.error(f"❌ Error uploading to S3: {e}")
        return False

def cleanup_temp_files(local_path):
    """Clean up temporary files"""
    try:
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info(f"🧹 Cleaned up temporary file: {local_path}")
        
        # Remove temp directory if empty
        temp_dir = os.path.dirname(local_path)
        if os.path.exists(temp_dir) and not os.listdir(temp_dir):
            os.rmdir(temp_dir)
            logger.info(f"🧹 Cleaned up temporary directory: {temp_dir}")
            
    except Exception as e:
        logger.warning(f"⚠️ Error cleaning up temp files: {e}")

def main():
    """Main function to extract and upload the ALS model"""
    print("🚀 ALS Model Extraction and S3 Upload")
    print("=" * 50)
    
    try:
        # Extract model from container
        local_path = extract_model_from_container()
        if not local_path:
            print("❌ Failed to extract model from container")
            return 1
        
        # Upload to S3
        s3_url = upload_model_to_s3(local_path)
        if not s3_url:
            print("❌ Failed to upload model to S3")
            return 1
        
        print(f"\n✅ Success! Model uploaded to: {s3_url}")
        
        # Clean up
        cleanup_temp_files(local_path)
        
        print("\n📋 Summary:")
        print(f"• Model extracted from container: /app/als_model.pkl")
        print(f"• Model uploaded to S3: {s3_url}")
        print(f"• File size: {os.path.getsize(local_path)} bytes")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 