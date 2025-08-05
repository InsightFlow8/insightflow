#!/usr/bin/env python3
"""
Test script to verify the load_s3_vector_store logic
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
    return original_import(name, *args, **kwargs)

builtins.__import__ = mock_import

# Now import our module
from vector_store_s3 import load_s3_vector_store, init_s3_vector_store

# Restore original import
builtins.__import__ = original_import

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def test_load_logic():
    """Test the load_s3_vector_store logic"""
    try:
        logger.info("🧪 Testing load_s3_vector_store logic...")
        
        # Test the load function
        logger.info("🔄 Testing load_s3_vector_store()...")
        store = load_s3_vector_store()
        
        if store:
            logger.info("✅ Store loaded successfully")
            logger.info(f"   Bucket: {store.bucket_name}")
            logger.info(f"   Index: {store.index_name}")
        else:
            logger.info("❌ Store loading failed")
            
        return store
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return None

if __name__ == "__main__":
    test_load_logic() 