#!/usr/bin/env python3
"""
Test script to check vector metadata in S3Vectors database
"""

import os
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def test_vector_metadata():
    """Test to see what metadata is stored in the S3Vectors database"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        logger.info(f"🔍 Checking vector metadata in S3Vectors database...")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Index: {index_name}")
        
        # Initialize S3Vectors client
        s3vectors_client = boto3.client('s3vectors')
        logger.info("✅ S3Vectors client initialized successfully")
        
        # List vectors
        logger.info("🔄 Listing vectors...")
        
        request_params = {
            'vectorBucketName': bucket_name,
            'indexName': index_name,
            'maxResults': 10
        }
        
        try:
            response = s3vectors_client.list_vectors(**request_params)
            logger.info("✅ list_vectors API call successful")
            
            vectors = response.get('vectors', [])
            logger.info(f"   Found {len(vectors)} vectors")
            
            if vectors:
                logger.info("   Vector details:")
                for i, vector in enumerate(vectors):
                    key = vector.get('key', 'Unknown')
                    metadata = vector.get('metadata', {})
                    
                    logger.info(f"     Vector {i+1}:")
                    logger.info(f"       Key: {key}")
                    logger.info(f"       Metadata keys: {list(metadata.keys())}")
                    logger.info(f"       Full metadata: {metadata}")
                    
                    # Check for product_id specifically
                    product_id = metadata.get('product_id')
                    if product_id:
                        logger.info(f"       ✅ Product ID found: {product_id}")
                    else:
                        logger.info(f"       ❌ No product_id found in metadata")
                    
                    # Check for other common fields
                    product_name = metadata.get('product_name')
                    if product_name:
                        logger.info(f"       ✅ Product name: {product_name}")
                    else:
                        logger.info(f"       ❌ No product_name found")
                    
                    logger.info("")  # Empty line for readability
            else:
                logger.info("   No vectors found")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ list_vectors API call failed: {e}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize S3Vectors client: {e}")
        return False

if __name__ == "__main__":
    success = test_vector_metadata()
    if success:
        logger.info("🎉 Vector metadata test completed!")
    else:
        logger.error("💥 Vector metadata test failed!") 