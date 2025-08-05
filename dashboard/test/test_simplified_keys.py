#!/usr/bin/env python3
"""
Test to verify that the simplified key format works correctly
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

def test_simplified_keys():
    """Test that the simplified key format works correctly"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing simplified key format...")
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
        
        # Test 1: Upload a test vector with simplified key
        logger.info("🔄 Test 1: Uploading test vector with simplified key...")
        
        test_vector = {
            'key': '999',  # Simplified key - just the product ID
            'data': {
                'float32': [0.1] * 1536
            },
            'metadata': {
                'product_id': '999',
                'product_name': 'Test Product 999',
                'category': 'test'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector]
            )
            logger.info("✅ Test vector uploaded successfully with simplified key")
            
        except Exception as e:
            logger.error(f"❌ Error uploading test vector: {e}")
            return False
        
        # Test 2: Retrieve the vector using simplified key
        logger.info("🔄 Test 2: Retrieving vector with simplified key...")
        
        try:
            response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['999'],  # Simplified key
                returnMetadata=True
            )
            
            vectors = response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata: {metadata}")
                
                if metadata.get('product_name') == 'Test Product 999':
                    logger.info("✅ Simplified key retrieval working!")
                else:
                    logger.warning("❌ Simplified key retrieval not working")
            else:
                logger.warning("   Vector not found with simplified key")
                
        except Exception as e:
            logger.error(f"❌ Error retrieving vector: {e}")
        
        # Test 3: Search and verify results use simplified keys
        logger.info("🔄 Test 3: Searching with simplified keys...")
        
        try:
            query_vector = [0.1] * 1536
            
            response = s3vectors_client.query_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                queryVector={"float32": query_vector},
                topK=5,
                returnMetadata=True
            )
            
            vectors = response.get('vectors', [])
            logger.info(f"   Search returned {len(vectors)} products")
            
            for i, vector in enumerate(vectors):
                key = vector.get('key', 'unknown')
                metadata = vector.get('metadata', {})
                logger.info(f"   Result {i+1}: Key='{key}', Name='{metadata.get('product_name', 'Unknown')}'")
                
                # Check if key is simplified (just a number)
                if key.isdigit():
                    logger.info(f"   ✅ Result {i+1} uses simplified key format!")
                else:
                    logger.info(f"   📝 Result {i+1} uses legacy key format: {key}")
                    
        except Exception as e:
            logger.error(f"❌ Error searching products: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test simplified keys: {e}")
        return False

if __name__ == "__main__":
    success = test_simplified_keys()
    if success:
        logger.info("🎉 Simplified keys test completed!")
    else:
        logger.error("💥 Simplified keys test failed!") 