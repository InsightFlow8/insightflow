#!/usr/bin/env python3
"""
Simple test to see if metadata works without explicit configuration
"""

import os
import boto3
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def test_metadata_simple():
    """Test if metadata works without explicit configuration"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing metadata without explicit configuration...")
        
        # Initialize S3Vectors client
        if aws_access_key and aws_secret_key:
            s3vectors_client = boto3.client(
                's3vectors',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            s3vectors_client = boto3.client('s3vectors', region_name=aws_region)
        
        # Test uploading a vector with metadata
        logger.info("🔄 Testing vector upload with metadata...")
        
        test_vector = {
            'key': 'test_product_888',
            'data': {
                'float32': [0.1] * 1536  # Mock embedding
            },
            'metadata': {
                'product_id': '888',
                'product_name': 'Test Product 888',
                'aisle': 'Test Aisle',
                'department': 'Test Department',
                'custom_field': 'custom_value'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector]
            )
            logger.info("✅ Test vector uploaded successfully")
            
            # Try to retrieve the test vector
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_product_888']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved vector metadata: {metadata}")
                
                if metadata.get('product_id') == '888':
                    logger.info("✅ Metadata is being saved and retrieved correctly!")
                    logger.info("💡 The issue was not with metadata configuration - it works without explicit config")
                else:
                    logger.warning("❌ Metadata is still not being saved correctly")
                    logger.info("💡 This suggests a deeper issue with the S3Vectors service")
            else:
                logger.warning("❌ Test vector not found after upload")
                
        except Exception as e:
            logger.error(f"❌ Error testing vector upload: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test metadata: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_simple()
    if success:
        logger.info("🎉 Metadata test completed!")
    else:
        logger.error("💥 Metadata test failed!") 