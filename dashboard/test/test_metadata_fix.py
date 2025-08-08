#!/usr/bin/env python3
"""
Test to fix S3Vectors metadata using the correct API structure
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

def test_metadata_fix():
    """Test metadata with correct API structure"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        logger.info(f"🔍 Testing metadata fix with correct API structure...")
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
        
        # Test 1: Try with documentation-style vector structure
        logger.info("🔄 Test 1: Testing with documentation-style vector structure...")
        
        # Create test vector with the structure from documentation
        test_vector_doc_style = {
            'key': 'test_doc_style',
            'vector': [0.1] * 1536,  # Direct vector array as in docs
            'metadata': {
                'product_id': 'test_123',
                'product_name': 'Test Product',
                'category': 'test'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_doc_style]
            )
            logger.info("✅ Vector with doc-style structure uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_doc_style']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata: {metadata}")
                
                if metadata.get('product_id') == 'test_123':
                    logger.info("✅ Documentation-style metadata is working!")
                else:
                    logger.warning("❌ Documentation-style metadata is not working")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error with doc-style structure: {e}")
        
        # Test 2: Try with our current structure but correct metadata
        logger.info("🔄 Test 2: Testing with our current structure...")
        
        test_vector_our_style = {
            'key': 'test_our_style',
            'data': {
                'float32': [0.2] * 1536
            },
            'metadata': {
                'product_id': 'test_456',
                'product_name': 'Test Product 2',
                'category': 'test'
            }
        }
        
        try:
            upload_response = s3vectors_client.put_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                vectors=[test_vector_our_style]
            )
            logger.info("✅ Vector with our style structure uploaded successfully")
            
            # Retrieve and check
            get_response = s3vectors_client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                keys=['test_our_style']
            )
            
            vectors = get_response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                logger.info(f"   Retrieved metadata: {metadata}")
                
                if metadata.get('product_id') == 'test_456':
                    logger.info("✅ Our style metadata is working!")
                else:
                    logger.warning("❌ Our style metadata is not working")
            else:
                logger.warning("   Vector not found")
                
        except Exception as e:
            logger.error(f"❌ Error with our style structure: {e}")
        
        # Test 3: Try querying with metadata filter
        logger.info("🔄 Test 3: Testing metadata filtering...")
        
        try:
            # Create a query vector
            query_vector = [0.15] * 1536
            
            # Query without filter
            query_response = s3vectors_client.query_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
                queryVector=query_vector,
                maxResults=5,
                returnDistance=True,
                returnMetadata=True
            )
            
            vectors = query_response.get('vectors', [])
            logger.info(f"   Query returned {len(vectors)} vectors")
            
            for i, vector in enumerate(vectors):
                metadata = vector.get('metadata', {})
                logger.info(f"   Vector {i+1} metadata: {metadata}")
            
            # Try with metadata filter (if supported)
            try:
                filter_response = s3vectors_client.query_vectors(
                    vectorBucketName=bucket_name,
                    indexName=index_name,
                    queryVector=query_vector,
                    maxResults=5,
                    filter={"category": "test"},
                    returnDistance=True,
                    returnMetadata=True
                )
                
                filter_vectors = filter_response.get('vectors', [])
                logger.info(f"   Filtered query returned {len(filter_vectors)} vectors")
                
            except Exception as filter_e:
                logger.warning(f"   Metadata filtering not supported: {filter_e}")
            
        except Exception as e:
            logger.error(f"❌ Error with querying: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to test metadata fix: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_fix()
    if success:
        logger.info("🎉 Metadata fix test completed!")
    else:
        logger.error("💥 Metadata fix test failed!") 