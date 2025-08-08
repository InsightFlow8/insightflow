#!/usr/bin/env python3
"""
Simple test script to check if product 45000 exists in S3Vectors
"""

import os
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def check_product_45000_exists():
    """Check if product 45000 exists in S3Vectors database"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        logger.info(f"🔍 Checking if product 45000 exists in bucket: {bucket_name}")
        logger.info(f"🔍 Using index: {index_name}")
        
        # Initialize S3Vectors client
        s3vectors_client = boto3.client('s3vectors')
        
        # Try to get the specific vector for product 45000
        try:
            response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=[f"product_45000"]
            )
            
            vectors = response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info("✅ Product 45000 found!")
                logger.info(f"   Product Name: {metadata.get('product_name', 'Unknown')}")
                logger.info(f"   Aisle: {metadata.get('aisle', 'Unknown')}")
                logger.info(f"   Department: {metadata.get('department', 'Unknown')}")
                logger.info(f"   Vector Key: {vector.get('key', 'Unknown')}")
                return True
            else:
                logger.info("❌ Product 45000 not found in get_vectors response")
                return False
                
        except Exception as e:
            logger.info(f"⚠️ Error getting vector for product 45000: {e}")
            
            # Try listing vectors to see if any exist
            try:
                logger.info("🔄 Trying to list vectors to check if database has data...")
                list_response = s3vectors_client.list_vectors(
                    vectorBucketName=bucket_name,
                    indexName=index_name,
                    maxResults=10
                )
                
                vectors = list_response.get('vectors', [])
                if vectors:
                    logger.info(f"✅ Found {len(vectors)} vectors in database")
                    logger.info("   Sample vector keys:")
                    for i, vector in enumerate(vectors[:5]):
                        logger.info(f"   {i+1}. {vector.get('key', 'Unknown')}")
                    
                    # Check if product 45000 is in the list
                    for vector in vectors:
                        metadata = vector.get('metadata', {})
                        if str(metadata.get('product_id', '')) == '45000':
                            logger.info("✅ Product 45000 found in list!")
                            logger.info(f"   Product Name: {metadata.get('product_name', 'Unknown')}")
                            return True
                    
                    logger.info("❌ Product 45000 not found in listed vectors")
                    return False
                else:
                    logger.info("❌ No vectors found in database")
                    return False
                    
            except Exception as list_error:
                logger.error(f"❌ Error listing vectors: {list_error}")
                return False
        
    except Exception as e:
        logger.error(f"❌ Failed to check product 45000: {e}")
        return False

if __name__ == "__main__":
    exists = check_product_45000_exists()
    if exists:
        logger.info("🎉 Product 45000 exists - vector database is loaded!")
    else:
        logger.info("💡 Product 45000 does not exist - vector database needs to be loaded") 