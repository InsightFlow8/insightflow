#!/usr/bin/env python3
"""
Script to download ALS model from S3
"""

import os
import boto3
from datetime import datetime

def list_models_in_s3():
    """List all ALS models in S3"""
    try:
        bucket_name = os.getenv("S3_VECTORS_BUCKET")
        if not bucket_name:
            print("❌ S3_VECTORS_BUCKET environment variable not set")
            return None
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        # List objects with models/ prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix="models/als_model_"
        )
        
        if 'Contents' not in response:
            print("❌ No models found in S3")
            return None
        
        models = []
        for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']
            last_modified = obj['LastModified']
            models.append({
                'key': key,
                'size': size,
                'last_modified': last_modified
            })
        
        # Sort by last modified (newest first)
        models.sort(key=lambda x: x['last_modified'], reverse=True)
        
        print("📋 Available models in S3:")
        for i, model in enumerate(models, 1):
            print(f"{i}. {model['key']}")
            print(f"   Size: {model['size']} bytes")
            print(f"   Last modified: {model['last_modified']}")
            print()
        
        return models
        
    except Exception as e:
        print(f"❌ Error listing models: {e}")
        return None

def download_model_from_s3(s3_key=None, local_path=None):
    """Download ALS model from S3"""
    try:
        bucket_name = os.getenv("S3_VECTORS_BUCKET")
        if not bucket_name:
            print("❌ S3_VECTORS_BUCKET environment variable not set")
            return False
        
        # If no key provided, list available models
        if not s3_key:
            models = list_models_in_s3()
            if not models:
                return False
            
            # Use the most recent model
            s3_key = models[0]['key']
            print(f"📥 Using most recent model: {s3_key}")
        
        # Set local path if not provided
        if not local_path:
            local_path = "als_model.pkl"
        
        print(f"📥 Downloading {s3_key} to {local_path}...")
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        # Download the file
        s3_client.download_file(bucket_name, s3_key, local_path)
        
        file_size = os.path.getsize(local_path)
        print(f"✅ Model downloaded: {local_path} ({file_size} bytes)")
        
        return local_path
        
    except Exception as e:
        print(f"❌ Error downloading model: {e}")
        return False

def main():
    """Main function"""
    print("📥 ALS Model Download from S3")
    print("=" * 40)
    
    # List available models
    models = list_models_in_s3()
    if not models:
        return 1
    
    # Download the most recent model
    local_path = download_model_from_s3()
    if not local_path:
        return 1
    
    print(f"\n✅ Success! Model downloaded to: {local_path}")
    return 0

if __name__ == "__main__":
    exit(main()) 