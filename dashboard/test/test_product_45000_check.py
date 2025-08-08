#!/usr/bin/env python3
"""
Test script to verify product 45000 existence check
"""

import os
import sys
import logging

# Add the backend directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from vector_store_s3 import check_product_exists_s3, initialize_s3_vector_store

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def test_product_45000_check():
    """Test the product 45000 existence check"""
    try:
        logger.info("🧪 Testing product 45000 existence check...")
        
        # Initialize the S3 Vectors store
        logger.info("🔄 Initializing S3 Vectors store...")
        initialize_s3_vector_store()
        
        # Check if product 45000 exists
        logger.info("🔍 Checking if product 45000 exists...")
        result = check_product_exists_s3(45000)
        
        logger.info(f"📊 Result: {result}")
        
        if result.get('exists', False):
            logger.info("✅ Product 45000 exists in the vector database")
            logger.info(f"   Product Name: {result.get('product_name', 'Unknown')}")
            logger.info(f"   Aisle: {result.get('aisle', 'Unknown')}")
            logger.info(f"   Department: {result.get('department', 'Unknown')}")
            logger.info(f"   Vector Key: {result.get('vector_key', 'Unknown')}")
        else:
            logger.info("❌ Product 45000 does not exist in the vector database")
            if 'error' in result:
                logger.error(f"   Error: {result['error']}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return {'exists': False, 'error': str(e)}

if __name__ == "__main__":
    test_product_45000_check() 