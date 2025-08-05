#!/usr/bin/env python3
"""
Final test to verify that the metadata fix works with our updated code
"""

import os
import sys
import logging

# Add the backend directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Mock the langchain_openai import to avoid dependency issues
import builtins
original_import = builtins.__import__

def mock_import(name, *args, **kwargs):
    if name == 'langchain_openai':
        # Create a mock OpenAIEmbeddings class
        class MockOpenAIEmbeddings:
            def __init__(self):
                pass
            def embed_documents(self, texts):
                return [[0.1] * 1536] * len(texts)  # Mock embeddings
            def embed_query(self, text):
                return [0.1] * 1536  # Mock query embedding
        
        # Create a mock module
        import types
        mock_module = types.ModuleType('langchain_openai')
        mock_module.OpenAIEmbeddings = MockOpenAIEmbeddings
        return mock_module
    elif name == 'langchain':
        # Create a mock langchain module
        import types
        mock_module = types.ModuleType('langchain')
        mock_module.schema = types.ModuleType('langchain.schema')
        mock_module.schema.Document = type('Document', (), {})
        return mock_module
    return original_import(name, *args, **kwargs)

builtins.__import__ = mock_import

# Now import our modules
from vector_store_s3 import init_s3_vector_store
from tools import get_product_details

# Restore original import
builtins.__import__ = original_import

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def test_metadata_fix_final():
    """Test that the metadata fix works with our updated code"""
    try:
        logger.info("🧪 Testing metadata fix with updated code...")
        
        # Initialize the S3 Vectors store
        logger.info("🔄 Initializing S3 Vectors store...")
        s3_store = init_s3_vector_store()
        
        if s3_store:
            logger.info("✅ S3 Vectors store initialized successfully")
            
            # Test 1: Check if product 3 exists using the updated method
            logger.info("🔍 Test 1: Checking if product 3 exists...")
            result = s3_store.check_product_exists(3)
            logger.info(f"   Product 3 check result: {result}")
            
            if result.get('exists'):
                logger.info("✅ Product 3 found with metadata!")
            else:
                logger.warning("❌ Product 3 not found")
            
            # Test 2: Test the get_product_details tool
            logger.info("🔍 Test 2: Testing get_product_details tool...")
            product_result = get_product_details("3")
            logger.info(f"   get_product_details result: {product_result}")
            
            if "Unknown Product" not in product_result:
                logger.info("✅ get_product_details working with metadata!")
            else:
                logger.warning("❌ get_product_details not working properly")
            
            # Test 3: Test search functionality
            logger.info("🔍 Test 3: Testing search functionality...")
            search_results = s3_store.search_similar_products("milk", 3)
            logger.info(f"   Search results: {len(search_results)} products found")
            
            for i, result in enumerate(search_results):
                metadata = result.get('metadata', {})
                logger.info(f"   Result {i+1}: {metadata.get('product_name', 'Unknown')}")
                
                if metadata.get('product_name'):
                    logger.info(f"   ✅ Result {i+1} has metadata!")
                else:
                    logger.warning(f"   ❌ Result {i+1} missing metadata")
            
            return True
        else:
            logger.error("❌ Failed to initialize S3 Vectors store")
            return False
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_metadata_fix_final()
    if success:
        logger.info("🎉 Metadata fix test completed successfully!")
    else:
        logger.error("💥 Metadata fix test failed!") 