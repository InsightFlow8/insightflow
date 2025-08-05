#!/usr/bin/env python3
"""
Test to verify that the search functionality works with correct API parameters
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

def test_search_fix():
    """Test that the search functionality works with correct API parameters"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing search functionality with correct API parameters...")
        logger.info(f"   Region: {aws_region}")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Index: {index_name}")
        
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
        
        # Test search with correct parameters
        logger.info("🔄 Testing search with correct API parameters...")
        
        try:
            # Create a simple query vector
            query_vector = [0.1] * 1536
            
            response = s3vectors_client.query_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                queryVector=query_vector,
                topK=3,
                returnMetadata=True
            )
            
            vectors = response.get('vectors', [])
            logger.info(f"   Search returned {len(vectors)} products")
            
            for i, vector in enumerate(vectors):
                metadata = vector.get('metadata', {})
                logger.info(f"   Result {i+1}: {metadata.get('product_name', 'Unknown')}")
                
                if metadata.get('product_name'):
                    logger.info(f"   ✅ Result {i+1} has metadata!")
                else:
                    logger.warning(f"   ❌ Result {i+1} missing metadata")
                    
        except Exception as e:
            logger.error(f"❌ Error searching products: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test search fix: {e}")
        return False

if __name__ == "__main__":
    success = test_search_fix()
    if success:
        logger.info("🎉 Search fix test completed!")
    else:
        logger.error("💥 Search fix test failed!") 