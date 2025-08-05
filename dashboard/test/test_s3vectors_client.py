#!/usr/bin/env python3
"""
Test script to check S3Vectors client functionality
"""

import os
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def test_s3vectors_client():
    """Test the S3Vectors client functionality"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        logger.info(f"🔍 Testing S3Vectors client...")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Index: {index_name}")
        
        # Initialize S3Vectors client
        s3vectors_client = boto3.client('s3vectors')
        logger.info("✅ S3Vectors client initialized successfully")
        
        # Test listing vectors
        logger.info("🔄 Testing list_vectors API call...")
        
        # Build request parameters
        request_params = {
            'vectorBucketName': bucket_name,
            'indexName': index_name,
            'maxResults': 10
        }
        
        logger.info(f"   Request params: {request_params}")
        
        try:
            response = s3vectors_client.list_vectors(**request_params)
            logger.info("✅ list_vectors API call successful")
            
            vectors = response.get('vectors', [])
            logger.info(f"   Found {len(vectors)} vectors")
            
            if vectors:
                logger.info("   Sample vectors:")
                for i, vector in enumerate(vectors[:3]):
                    key = vector.get('key', 'Unknown')
                    metadata = vector.get('metadata', {})
                    product_name = metadata.get('product_name', 'Unknown')
                    logger.info(f"     {i+1}. Key: {key}, Product: {product_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ list_vectors API call failed: {e}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize S3Vectors client: {e}")
        return False

if __name__ == "__main__":
    success = test_s3vectors_client()
    if success:
        logger.info("🎉 S3Vectors client test passed!")
    else:
        logger.error("💥 S3Vectors client test failed!") 