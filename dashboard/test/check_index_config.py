#!/usr/bin/env python3
"""
Script to check the S3Vectors index configuration
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

def check_index_config():
    """Check the S3Vectors index configuration"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Checking index configuration...")
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
        
        # Get index details
        try:
            response = s3vectors_client.get_index(
                vectorBucketName=bucket_name,
                indexName=index_name
            )
            
            logger.info("✅ Index details:")
            logger.info(f"   Index Name: {response.get('indexName')}")
            logger.info(f"   Data Type: {response.get('dataType')}")
            logger.info(f"   Dimension: {response.get('dimension')}")
            logger.info(f"   Distance Metric: {response.get('distanceMetric')}")
            
            # Check metadata configuration
            metadata_config = response.get('metadataConfiguration', {})
            logger.info(f"   Metadata Configuration: {metadata_config}")
            
            non_filterable_keys = metadata_config.get('nonFilterableMetadataKeys', [])
            logger.info(f"   Non-filterable metadata keys: {non_filterable_keys}")
            
            if 'product_id' in non_filterable_keys:
                logger.info("✅ product_id is configured in metadata")
            else:
                logger.warning("❌ product_id is NOT configured in metadata")
            
            if 'product_name' in non_filterable_keys:
                logger.info("✅ product_name is configured in metadata")
            else:
                logger.warning("❌ product_name is NOT configured in metadata")
            
            # Test uploading a vector with metadata
            logger.info("🔄 Testing vector upload with metadata...")
            
            test_vector = {
                'key': 'test_product_999',
                'data': {
                    'float32': [0.1] * 1536  # Mock embedding
                },
                'metadata': {
                    'product_id': '999',
                    'product_name': 'Test Product',
                    'aisle': 'Test Aisle',
                    'department': 'Test Department'
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
                    keys=['test_product_999']
                )
                
                vectors = get_response.get('vectors', [])
                if vectors:
                    vector = vectors[0]
                    metadata = vector.get('metadata', {})
                    logger.info(f"   Retrieved vector metadata: {metadata}")
                    
                    if metadata.get('product_id') == '999':
                        logger.info("✅ Metadata is being saved and retrieved correctly")
                    else:
                        logger.warning("❌ Metadata is not being saved correctly")
                else:
                    logger.warning("❌ Test vector not found after upload")
                    
            except Exception as e:
                logger.error(f"❌ Error testing vector upload: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error getting index details: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to check index configuration: {e}")
        return False

if __name__ == "__main__":
    success = check_index_config()
    if success:
        logger.info("🎉 Index configuration check completed!")
    else:
        logger.error("💥 Index configuration check failed!") 