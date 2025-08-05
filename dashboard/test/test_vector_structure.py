#!/usr/bin/env python3
"""
Test different vector structures to see if the issue is with the data format
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

def test_vector_structure():
    """Test different vector structures"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing different vector structures...")
        
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
        
        # Test 1: Standard structure
        logger.info("🔄 Test 1: Standard vector structure...")
        test_vector_1 = {
            'key': 'test_product_777',
            'data': {
                'float32': [0.1] * 1536
            },
            'metadata': {
                'product_id': '777',
                'product_name': 'Test Product 777'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_1]
            )
            logger.info("✅ Test 1 vector uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_product_777']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Test 1 metadata: {metadata}")
            else:
                logger.warning("   Test 1 vector not found")
                
        except Exception as e:
            logger.error(f"❌ Test 1 failed: {e}")
        
        # Test 2: Different metadata structure
        logger.info("🔄 Test 2: Different metadata structure...")
        test_vector_2 = {
            'key': 'test_product_666',
            'data': {
                'float32': [0.2] * 1536
            },
            'metadata': {
                'id': '666',
                'name': 'Test Product 666',
                'type': 'product'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_2]
            )
            logger.info("✅ Test 2 vector uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_product_666']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Test 2 metadata: {metadata}")
            else:
                logger.warning("   Test 2 vector not found")
                
        except Exception as e:
            logger.error(f"❌ Test 2 failed: {e}")
        
        # Test 3: List all vectors to see what's actually stored
        logger.info("🔄 Test 3: Listing all vectors to see what's stored...")
        try:
            list_response = s3vectors_client.list_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                maxResults=10
            )
            
            vectors = list_response.get('vectors', [])
            logger.info(f"   Found {len(vectors)} vectors in index")
            
            for i, vector in enumerate(vectors):
                key = vector.get('key', 'Unknown')
                metadata = vector.get('metadata', {})
                logger.info(f"   Vector {i+1}: Key={key}, Metadata={metadata}")
                
        except Exception as e:
            logger.error(f"❌ Test 3 failed: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test vector structures: {e}")
        return False

if __name__ == "__main__":
    success = test_vector_structure()
    if success:
        logger.info("🎉 Vector structure test completed!")
    else:
        logger.error("💥 Vector structure test failed!") 