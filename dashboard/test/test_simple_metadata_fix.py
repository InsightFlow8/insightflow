#!/usr/bin/env python3
"""
Simple test to verify the metadata fix works with S3Vectors API
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

def test_simple_metadata_fix():
    """Test that the metadata fix works with S3Vectors API"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing simple metadata fix...")
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
        
        # Test 1: Check if product 3 exists with returnMetadata=True
        logger.info("🔄 Test 1: Checking if product 3 exists with returnMetadata=True...")
        
        try:
            response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['product_3'],
                returnMetadata=True
            )
            
            vectors = response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Product 3 metadata: {metadata}")
                
                if metadata.get('product_name'):
                    logger.info("✅ Product 3 found with metadata!")
                else:
                    logger.warning("❌ Product 3 found but no metadata")
            else:
                logger.warning("   Product 3 not found")
                
        except Exception as e:
            logger.error(f"❌ Error checking product 3: {e}")
        
        # Test 2: Search for products with returnMetadata=True
        logger.info("🔄 Test 2: Searching for products with returnMetadata=True...")
        
        try:
            # Create a simple query vector
            query_vector = [0.1] * 1536
            
            response = s3vectors_client.query_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vector=query_vector,
                k=3,
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
        
        # Test 3: List vectors with returnMetadata=True
        logger.info("🔄 Test 3: Listing vectors with returnMetadata=True...")
        
        try:
            response = s3vectors_client.list_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                maxResults=3,
                returnMetadata=True
            )
            
            vectors = response.get('vectors', [])
            logger.info(f"   List returned {len(vectors)} vectors")
            
            for i, vector in enumerate(vectors):
                metadata = vector.get('metadata', {})
                logger.info(f"   Vector {i+1}: {metadata.get('product_name', 'Unknown')}")
                
                if metadata.get('product_name'):
                    logger.info(f"   ✅ Vector {i+1} has metadata!")
                else:
                    logger.warning(f"   ❌ Vector {i+1} missing metadata")
                    
        except Exception as e:
            logger.error(f"❌ Error listing vectors: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test simple metadata fix: {e}")
        return False

if __name__ == "__main__":
    success = test_simple_metadata_fix()
    if success:
        logger.info("🎉 Simple metadata fix test completed!")
    else:
        logger.error("💥 Simple metadata fix test failed!") 