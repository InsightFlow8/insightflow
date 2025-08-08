#!/usr/bin/env python3
"""
Script to list all S3Vectors buckets and indexes
"""

import os
import boto3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def list_s3vectors_resources():
    """List all S3Vectors buckets and indexes"""
    try:
        logger.info("🔍 Listing S3Vectors resources...")
        
        # Initialize S3Vectors client
        s3vectors_client = boto3.client('s3vectors')
        logger.info("✅ S3Vectors client initialized successfully")
        
        # List vector buckets
        logger.info("🔄 Listing vector buckets...")
        try:
            buckets_response = s3vectors_client.list_vector_buckets()
            buckets = buckets_response.get('vectorBuckets', [])
            logger.info(f"   Found {len(buckets)} vector buckets:")
            
            for bucket in buckets:
                bucket_name = bucket.get('vectorBucketName', 'Unknown')
                logger.info(f"     - {bucket_name}")
                
                # List indexes for this bucket
                try:
                    indexes_response = s3vectors_client.list_indexes(
                        vectorBucketName=bucket_name
                    )
                    indexes = indexes_response.get('indexes', [])
                    logger.info(f"       Indexes in {bucket_name}: {len(indexes)}")
                    
                    for index in indexes:
                        index_name = index.get('indexName', 'Unknown')
                        logger.info(f"         - {index_name}")
                        
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
    success = list_s3vectors_resources()
    if success:
        logger.info("🎉 S3Vectors resource listing completed!")
    else:
        logger.error("💥 S3Vectors resource listing failed!") 