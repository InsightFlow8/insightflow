#!/usr/bin/env python3
"""
Test to check S3Vectors preview version limitations
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

def test_preview_limitations():
    """Test to understand S3Vectors preview limitations"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing S3Vectors preview limitations...")
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
        
        # Test 1: Check if we can get service information
        logger.info("🔄 Test 1: Checking service information...")
        try:
            # Try to get the index details to see what's supported
            response = s3vectors_client.get_index(
                vectorBucketName=bucket_name,
                indexName=index_name
            )
            
            logger.info("✅ Index details retrieved:")
            logger.info(f"   Index Name: {response.get('indexName')}")
            logger.info(f"   Data Type: {response.get('dataType')}")
            logger.info(f"   Dimension: {response.get('dimension')}")
            logger.info(f"   Distance Metric: {response.get('distanceMetric')}")
            logger.info(f"   Full Response: {response}")
            
        except Exception as e:
            logger.error(f"❌ Error getting index details: {e}")
        
        # Test 2: Try a simple vector upload without metadata
        logger.info("🔄 Test 2: Testing vector upload without metadata...")
        test_vector_no_metadata = {
            'key': 'test_no_metadata',
            'data': {
                'float32': [0.1] * 1536
            }
            # No metadata field
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_no_metadata]
            )
            logger.info("✅ Vector without metadata uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_no_metadata']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                logger.info(f"   Retrieved vector: {vector}")
                logger.info(f"   Has metadata field: {'metadata' in vector}")
                if 'metadata' in vector:
                    logger.info(f"   Metadata content: {vector['metadata']}")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error with vector without metadata: {e}")
        
        # Test 3: Try with minimal metadata
        logger.info("🔄 Test 3: Testing with minimal metadata...")
        test_vector_minimal = {
            'key': 'test_minimal_metadata',
            'data': {
                'float32': [0.2] * 1536
            },
            'metadata': {
                'test': 'value'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_minimal]
            )
            logger.info("✅ Vector with minimal metadata uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_minimal_metadata']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata: {metadata}")
                
                if metadata.get('test') == 'value':
                    logger.info("✅ Minimal metadata is working!")
                else:
                    logger.warning("❌ Minimal metadata is not working")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error with minimal metadata: {e}")
        
        # Test 4: Check what regions support S3Vectors
        logger.info("🔄 Test 4: Checking S3Vectors availability...")
        logger.info("   S3Vectors is in preview release - this might explain metadata issues")
        logger.info("   Preview services often have limited functionality")
        logger.info("   Metadata support might not be fully implemented yet")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test preview limitations: {e}")
        return False

if __name__ == "__main__":
    success = test_preview_limitations()
    if success:
        logger.info("🎉 Preview limitations test completed!")
        logger.info("💡 S3Vectors is in preview - metadata might not be fully supported")
    else:
        logger.error("💥 Preview limitations test failed!") 