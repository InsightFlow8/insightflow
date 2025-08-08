#!/usr/bin/env python3
"""
Script to recreate the S3Vectors index with proper metadata configuration
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

def recreate_index():
    """Recreate the S3Vectors index with proper metadata configuration"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Recreating index with proper metadata configuration...")
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
        
        # Step 1: Delete the existing index
        logger.info("🔄 Step 1: Deleting existing index...")
        try:
            s3vectors_client.delete_index(
                vectorBucketName=bucket_name,
                indexName=index_name
            )
            logger.info("✅ Existing index deleted successfully")
        except Exception as e:
            logger.warning(f"⚠️ Error deleting index (might not exist): {e}")
        
        # Step 2: Create new index with proper metadata configuration
        logger.info("🔄 Step 2: Creating new index with metadata configuration...")
        try:
            response = s3vectors_client.create_index(
                vectorBucketName=bucket_name,
                indexName=index_name,
                dataType='float32',
                dimension=1536,
                distanceMetric='cosine',
                metadataConfiguration={
                    'nonFilterableMetadataKeys': [
                        'product_id',
                        'product_name', 
                        'aisle',
                        'department',
                        'aisle_id',
                        'department_id',
                        'content'
                    ]
                }
            )
            logger.info("✅ New index created with metadata configuration")
            
            # Step 3: Verify the index configuration
            logger.info("🔄 Step 3: Verifying index configuration...")
            index_response = s3vectors_client.get_index(
                vectorBucketName=bucket_name,
                indexName=index_name
            )
            
            metadata_config = index_response.get('metadataConfiguration', {})
            non_filterable_keys = metadata_config.get('nonFilterableMetadataKeys', [])
            
            logger.info(f"   Metadata Configuration: {metadata_config}")
            logger.info(f"   Non-filterable metadata keys: {non_filterable_keys}")
            
            if 'product_id' in non_filterable_keys:
                logger.info("✅ product_id is now configured in metadata")
            else:
                logger.error("❌ product_id is still NOT configured in metadata")
            
            if 'product_name' in non_filterable_keys:
                logger.info("✅ product_name is now configured in metadata")
            else:
                logger.error("❌ product_name is still NOT configured in metadata")
            
            # Step 4: Test uploading a vector with metadata
            logger.info("🔄 Step 4: Testing vector upload with metadata...")
            
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
                    logger.info("✅ Metadata is now being saved and retrieved correctly!")
                else:
                    logger.error("❌ Metadata is still not being saved correctly")
            else:
                logger.error("❌ Test vector not found after upload")
            
        except Exception as e:
            logger.error(f"❌ Error creating new index: {e}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to recreate index: {e}")
        return False

if __name__ == "__main__":
    success = recreate_index()
    if success:
        logger.info("🎉 Index recreation completed successfully!")
        logger.info("💡 Now you can restart the Docker container to reload the data with proper metadata.")
    else:
        logger.error("💥 Index recreation failed!") 