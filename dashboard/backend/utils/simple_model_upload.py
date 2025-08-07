#!/usr/bin/env python3
"""
Simple script to extract ALS model and upload to S3
"""

import os
import boto3
import subprocess
from datetime import datetime

def main():
    print("🚀 Simple ALS Model Upload to S3")
    print("=" * 40)
    
    # Get container name
    container_name = "dashboard-backend-1"  # Default Docker Compose container name
    
    # Create temp directory
    os.makedirs("temp_models", exist_ok=True)
    local_path = "temp_models/als_model.pkl"
    
    try:
        # Step 1: Extract model from container
        print("📥 Extracting model from container...")
        cmd = f"docker cp {container_name}:/app/als_model.pkl {local_path}"
        print(f"Running: {cmd}")
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Failed to extract model: {result.stderr}")
            return 1
        
        if not os.path.exists(local_path):
            print("❌ Model file not found after extraction")
            return 1
        
        file_size = os.path.getsize(local_path)
        print(f"✅ Model extracted: {local_path} ({file_size} bytes)")
        
        # Step 2: Upload to S3
        print("📤 Uploading to S3...")
        
        # Get S3 configuration
        bucket_name = os.getenv("S3_VECTORS_BUCKET")
        if not bucket_name:
            print("❌ S3_VECTORS_BUCKET environment variable not set")
            return 1
        
        # Generate S3 key
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"models/als_model_{timestamp}.pkl"
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        # Upload file
        s3_client.upload_file(local_path, bucket_name, s3_key)
        
        s3_url = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ Model uploaded to: {s3_url}")
        
        # Clean up
        os.remove(local_path)
        print("🧹 Cleaned up temporary file")
        
        print(f"\n📋 Summary:")
        print(f"• Model extracted from: {container_name}:/app/als_model.pkl")
        print(f"• Model uploaded to: {s3_url}")
        print(f"• File size: {file_size} bytes")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 