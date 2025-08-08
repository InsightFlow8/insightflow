#!/usr/bin/env python3
"""
Test script to verify S3Vectors API methods
"""
import os
import sys
import logging

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_s3vectors_api_methods():
    """Test S3Vectors API methods"""
    try:
        import boto3
        
        print("🔄 Testing S3Vectors API methods...")
        
        # Create S3Vectors client
        client = boto3.client('s3vectors')
        print("✅ S3Vectors client created successfully")
        
        # Test available methods
        print("\n📋 Available S3Vectors methods:")
        methods = [method for method in dir(client) if not method.startswith('_')]
        for method in sorted(methods):
            print(f"  - {method}")
        
        # Test specific methods we need
        bucket_name = "test-bucket-12345"
        index_name = "test-index-12345"
        
        print(f"\n🧪 Testing with bucket: {bucket_name}, index: {index_name}")
        
        # Test bucket methods
        try:
            print("🔄 Testing list_vector_buckets...")
            response = client.list_vector_buckets()
            print(f"✅ list_vector_buckets works: {len(response.get('vectorBuckets', []))} buckets found")
        except Exception as e:
            print(f"❌ list_vector_buckets failed: {e}")
        
        # Test index methods
        try:
            print("🔄 Testing list_indexes...")
            response = client.list_indexes(vectorBucketName=bucket_name)
            print(f"✅ list_indexes works: {len(response.get('indexes', []))} indexes found")
        except Exception as e:
            print(f"❌ list_indexes failed: {e}")
        
        # Test get methods
        try:
            print("🔄 Testing get_vector_bucket...")
            response = client.get_vector_bucket(vectorBucketName=bucket_name)
            print("✅ get_vector_bucket works")
        except Exception as e:
            print(f"❌ get_vector_bucket failed: {e}")
        
        try:
            print("🔄 Testing get_index...")
            response = client.get_index(vectorBucketName=bucket_name, indexName=index_name)
            print("✅ get_index works")
        except Exception as e:
            print(f"❌ get_index failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during S3Vectors API testing: {e}")
        return False

if __name__ == "__main__":
    print("🧪 S3Vectors API Methods Test")
    print("=" * 50)
    
    if test_s3vectors_api_methods():
        print("\n🎉 API methods test completed!")
    else:
        print("\n❌ API methods test failed!")
        sys.exit(1) 