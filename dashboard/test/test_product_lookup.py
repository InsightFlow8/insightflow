#!/usr/bin/env python3
"""
Test script to verify product lookup functionality
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
from tools import get_cached_product_lookup, get_product_details
from vector_store_s3 import initialize_s3_vector_store

# Restore original import
builtins.__import__ = original_import

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def test_product_lookup():
    """Test the product lookup functionality"""
    try:
        logger.info("🧪 Testing product lookup functionality...")
        
        # Initialize the S3 Vectors store
        logger.info("🔄 Initializing S3 Vectors store...")
        initialize_s3_vector_store()
        
        # Test the product lookup
        logger.info("🔍 Testing get_cached_product_lookup()...")
        product_lookup = get_cached_product_lookup()
        
        if product_lookup:
            logger.info(f"✅ Product lookup created successfully with {len(product_lookup)} products")
            
            # Test getting product details for product ID 3
            logger.info("🔍 Testing get_product_details for product ID 3...")
            result = get_product_details("3")
            logger.info(f"📊 Result: {result}")
            
            # Test with quoted product ID
            logger.info("🔍 Testing get_product_details for quoted product ID...")
            result2 = get_product_details("'3'")
            logger.info(f"📊 Result: {result2}")
            
            # Show some sample products
            sample_products = list(product_lookup.keys())[:5]
            logger.info(f"   Sample product IDs: {sample_products}")
            
            return product_lookup
        else:
            logger.info("❌ Product lookup is empty")
            return None
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return None

if __name__ == "__main__":
    test_product_lookup() 