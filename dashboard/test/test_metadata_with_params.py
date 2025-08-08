#!/usr/bin/env python3
"""
Test S3Vectors metadata with correct parameters from official documentation
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

def test_metadata_with_correct_params():
    """Test metadata with correct parameters from official documentation"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing metadata with correct parameters...")
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
        
        # Test 1: Upload vector with metadata
        logger.info("🔄 Test 1: Uploading vector with metadata...")
        
        test_vector = {
            'key': 'test_metadata_params',
            'data': {
                'float32': [0.1] * 1536
            },
            'metadata': {
                'product_id': 'test_789',
                'product_name': 'Test Product with Params',
                'category': 'test'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector]
            )
            logger.info("✅ Vector with metadata uploaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Error uploading vector: {e}")
            return False
        
        # Test 2: Get vector WITHOUT returnMetadata (default behavior)
        logger.info("🔄 Test 2: Getting vector WITHOUT returnMetadata (default)...")
        
        try:
            get_response_default = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_metadata_params']
                # returnMetadata defaults to False
            )
            
            vectors = get_response_default.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata (default): {metadata}")
                
                if metadata:
                    logger.info("✅ Metadata returned even with default parameters!")
                else:
                    logger.info("❌ No metadata returned with default parameters")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error getting vector (default): {e}")
        
        # Test 3: Get vector WITH returnMetadata=True
        logger.info("🔄 Test 3: Getting vector WITH returnMetadata=True...")
        
        try:
            get_response_with_metadata = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_metadata_params'],
                returnMetadata=True  # Explicitly request metadata
            )
            
            vectors = get_response_with_metadata.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata (explicit): {metadata}")
                
                if metadata.get('product_id') == 'test_789':
                    logger.info("✅ Metadata working with returnMetadata=True!")
                else:
                    logger.warning("❌ Metadata not working even with returnMetadata=True")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error getting vector (explicit): {e}")
        
        # Test 4: Get vector WITH returnMetadata=True AND returnData=True
        logger.info("🔄 Test 4: Getting vector WITH both returnMetadata=True AND returnData=True...")
        
        try:
            get_response_full = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_metadata_params'],
                returnMetadata=True,
                returnData=True
            )
            
            vectors = get_response_full.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                data = vector.get('data', {})
                logger.info(f"   Retrieved metadata (full): {metadata}")
                logger.info(f"   Has data: {'data' in vector}")
                logger.info(f"   Data type: {type(data)}")
                
                if metadata.get('product_id') == 'test_789':
                    logger.info("✅ Full response working with both parameters!")
                else:
                    logger.warning("❌ Metadata not working in full response")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error getting vector (full): {e}")
        
        # Test 5: Check if our existing vectors have metadata
        logger.info("🔄 Test 5: Checking existing vectors with returnMetadata=True...")
        
        try:
            # Get a few existing vectors
            list_response = s3vectors_client.list_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                maxResults=3
            )
            
            existing_vectors = list_response.get('vectors', [])
            if existing_vectors:
                logger.info(f"   Found {len(existing_vectors)} existing vectors")
                
                for i, vector_info in enumerate(existing_vectors):
                    vector_key = vector_info.get('key', 'unknown')
                    logger.info(f"   Checking vector {i+1}: {vector_key}")
                    
                    # Get full vector with metadata
                    get_response = s3vectors_client.get_vectors(
                        vectorBucketName=bucket_name,
                        indexName=index_name,
                        keys=[vector_key],
                        returnMetadata=True
                    )
                    
                    vectors = get_response.get('vectors', [])
                    if vectors:
                        vector = vectors[0]
                        metadata = vector.get('metadata', {})
                        logger.info(f"   Vector {vector_key} metadata: {metadata}")
                        
                        if metadata:
                            logger.info(f"   ✅ Vector {vector_key} has metadata!")
                        else:
                            logger.info(f"   ❌ Vector {vector_key} has no metadata")
                    else:
                        logger.warning(f"   Vector {vector_key} not found")
            else:
                logger.warning("   No existing vectors found")
                
        except Exception as e:
            logger.error(f"❌ Error checking existing vectors: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test metadata with correct params: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_with_correct_params()
    if success:
        logger.info("🎉 Metadata parameters test completed!")
    else:
        logger.error("💥 Metadata parameters test failed!") 