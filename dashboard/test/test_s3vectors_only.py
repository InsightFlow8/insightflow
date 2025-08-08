#!/usr/bin/env python3
"""
Test script for S3Vectors-only implementation
"""
import os
import sys
import logging

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_s3vectors_client():
    """Test S3Vectors client initialization"""
    try:
        import boto3
        
        print("🔄 Testing S3Vectors client initialization...")
        
        # Try to create S3Vectors client
        client = boto3.client('s3vectors')
        print("✅ S3Vectors client created successfully")
        
        # Test basic operations
        try:
            # List vector buckets
            response = client.list_vector_buckets()
            print(f"✅ Successfully listed vector buckets: {len(response.get('vectorBuckets', []))} found")
        except Exception as e:
            print(f"⚠️  Could not list vector buckets: {e}")
            
        return True
        
    except Exception as e:
        print(f"❌ Failed to create S3Vectors client: {e}")
        return False

def test_environment_variables():
    """Test that required environment variables are set"""
    print("🔄 Checking environment variables...")
    
    required_vars = [
        'OPENAI_API_KEY',
        'AWS_ACCESS_KEY_ID', 
        'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your .env file")
        return False
    else:
        print("✅ All required environment variables are set")
        return True

def test_s3vectors_store_creation():
    """Test S3Vectors store creation"""
    try:
        from vector_store_s3 import S3VectorsStore
        
        print("🔄 Testing S3Vectors store creation...")
        
        # Create store instance
        store = S3VectorsStore()
        print("✅ S3Vectors store instance created")
        
        # Test bucket creation
        store.create_vector_bucket()
        print("✅ Vector bucket creation successful")
        
        # Test index creation
        store.create_vector_index()
        print("✅ Vector index creation successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during S3Vectors store creation: {e}")
        return False

if __name__ == "__main__":
    print("🧪 S3Vectors-Only Implementation Test")
    print("=" * 50)
    
    # Test environment variables
    if not test_environment_variables():
        sys.exit(1)
    
    # Test S3Vectors client
    if not test_s3vectors_client():
        print("\n❌ S3Vectors client test failed!")
        print("This might indicate:")
        print("1. S3Vectors service is not available in your region")
        print("2. AWS credentials don't have S3Vectors permissions")
        print("3. boto3 version doesn't support S3Vectors")
        sys.exit(1)
    
    # Test store creation
    if test_s3vectors_store_creation():
        print("\n🎉 All tests passed!")
        print("S3Vectors-only implementation is working correctly")
    else:
        print("\n❌ Store creation test failed!")
        sys.exit(1) 