#!/usr/bin/env python3
"""
Test the new product lookup system
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
from product_lookup import get_product_by_id, get_all_products, search_products
from vector_store_s3 import init_s3_vector_store

# Restore original import
builtins.__import__ = original_import

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def test_new_product_lookup():
    """Test the new product lookup system"""
    try:
        logger.info("🧪 Testing new product lookup system...")
        
        # Initialize the S3 Vectors store (this will also build the product lookup)
        logger.info("🔄 Initializing S3 Vectors store...")
        s3_store = init_s3_vector_store()
        
        if s3_store:
            logger.info("✅ S3 Vectors store initialized successfully")
            
            # Test getting product by ID
            logger.info("🔍 Testing get_product_by_id...")
            product_3 = get_product_by_id("3")
            if product_3:
                logger.info(f"✅ Found product 3: {product_3.get('product_name', 'Unknown')}")
            else:
                logger.warning("❌ Product 3 not found")
            
            # Test getting all products
            logger.info("🔍 Testing get_all_products...")
            all_products = get_all_products()
            logger.info(f"✅ Found {len(all_products)} products")
            
            # Show some sample products
            sample_products = list(all_products.keys())[:5]
            logger.info(f"   Sample product IDs: {sample_products}")
            
            for product_id in sample_products:
                product = all_products[product_id]
                logger.info(f"   Product {product_id}: {product.get('product_name', 'Unknown')}")
            
            # Test search functionality
            logger.info("🔍 Testing search_products...")
            search_results = search_products("milk", limit=3)
            logger.info(f"✅ Found {len(search_results)} products matching 'milk'")
            
            for result in search_results:
                logger.info(f"   - {result.get('product_name', 'Unknown')} (ID: {result.get('product_id', 'Unknown')})")
            
            return True
        else:
            logger.error("❌ Failed to initialize S3 Vectors store")
            return False
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_new_product_lookup()
    if success:
        logger.info("🎉 New product lookup test completed successfully!")
    else:
        logger.error("💥 New product lookup test failed!") 