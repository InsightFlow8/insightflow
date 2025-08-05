#!/usr/bin/env python3
"""
Test script to check S3Vectors with environment variables from .env file
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

def test_s3vectors_with_env():
    """Test S3Vectors with environment variables from .env file"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing S3Vectors with environment variables...")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Index: {index_name}")
        logger.info(f"   Region: {aws_region}")
        logger.info(f"   AWS Access Key: {'Set' if aws_access_key else 'Not set'}")
        logger.info(f"   AWS Secret Key: {'Set' if aws_secret_key else 'Not set'}")
        
        # Initialize S3Vectors client with explicit credentials
        if aws_access_key and aws_secret_key:
            s3vectors_client = boto3.client(
                's3vectors',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            logger.info("✅ S3Vectors client initialized with explicit credentials")
        else:
            s3vectors_client = boto3.client('s3vectors', region_name=aws_region)
            logger.info("✅ S3Vectors client initialized with default credentials")
        
        # Test listing vector buckets
        logger.info("🔄 Testing list_vector_buckets...")
        try:
            buckets_response = s3vectors_client.list_vector_buckets()
            buckets = buckets_response.get('vectorBuckets', [])
            logger.info(f"   Found {len(buckets)} vector buckets:")
            
            for bucket in buckets:
                bucket_name_found = bucket.get('vectorBucketName', 'Unknown')
                logger.info(f"     - {bucket_name_found}")
                
                if bucket_name_found == bucket_name:
                    # List indexes for this bucket
                    try:
                        indexes_response = s3vectors_client.list_indexes(
                            vectorBucketName=bucket_name
                        )
                        indexes = indexes_response.get('indexes', [])
                        logger.info(f"       Indexes in {bucket_name}: {len(indexes)}")
                        
                        for index in indexes:
                            index_name_found = index.get('indexName', 'Unknown')
                            logger.info(f"         - {index_name_found}")
                            
                            if index_name_found == index_name:
                                # Try to list vectors in this index
                                try:
                                    vectors_response = s3vectors_client.list_vectors(
                                        vectorBucketName=bucket_name,
                                        indexName=index_name,
                                        maxResults=5
                                    )
                                    vectors = vectors_response.get('vectors', [])
                                    logger.info(f"           Vectors: {len(vectors)}")
                                    
                                    if vectors:
                                        logger.info("           Sample vectors:")
                                        for i, vector in enumerate(vectors[:3]):
                                            key = vector.get('key', 'Unknown')
                                            metadata = vector.get('metadata', {})
                                            logger.info(f"             {i+1}. Key: {key}")
                                            logger.info(f"                Metadata keys: {list(metadata.keys())}")
                                            if 'product_id' in metadata:
                                                logger.info(f"                Product ID: {metadata['product_id']}")
                                            if 'product_name' in metadata:
                                                logger.info(f"                Product Name: {metadata['product_name']}")
                                    
                                except Exception as e:
                                    logger.warning(f"           Error listing vectors: {e}")
                        
                    except Exception as e:
                        logger.warning(f"       Error listing indexes for {bucket_name}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error listing vector buckets: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize S3Vectors client: {e}")
        return False

if __name__ == "__main__":
    success = test_s3vectors_with_env()
    if success:
        logger.info("🎉 S3Vectors test with .env completed!")
    else:
        logger.error("💥 S3Vectors test with .env failed!") 