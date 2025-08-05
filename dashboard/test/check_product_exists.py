#!/usr/bin/env python3
"""
Script to check if a specific product exists in S3Vectors database
"""
import os
import sys
import logging
import boto3

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def check_product_exists(product_id: int):
    """Check if a specific product exists in S3Vectors database"""
    try:
        from vector_store_s3 import get_s3_vector_store
        
        print(f"🔄 Checking if product {product_id} exists in S3Vectors database...")
        
        # Get the S3Vectors store
        store = get_s3_vector_store()
        if not store:
            print("❌ S3Vectors store not initialized")
            return False
        
        # Method 1: Try to get the specific vector by key
        try:
            print(f"🔄 Method 1: Checking by vector key 'product_{product_id}'...")
            
            # Use get_vectors to check if the specific vector exists
            response = store.s3vectors_client.get_vectors(
                vectorBucketName=store.bucket_name,
                indexName=store.index_name,
                keys=[f"product_{product_id}"]
            )
            
            vectors = response.get('vectors', [])
            if vectors:
                vector = vectors[0]
                metadata = vector.get('metadata', {})
                print(f"✅ Product {product_id} found!")
                print(f"  - Product Name: {metadata.get('product_name', 'Unknown')}")
                print(f"  - Aisle: {metadata.get('aisle', 'Unknown')}")
                print(f"  - Department: {metadata.get('department', 'Unknown')}")
                print(f"  - Vector Key: {vector.get('key', 'Unknown')}")
                return True
            else:
                print(f"❌ Product {product_id} not found")
                return False
                
        except Exception as e:
            print(f"❌ Error checking by key: {e}")
            
            # Method 2: List all vectors and search
            try:
                print(f"🔄 Method 2: Listing all vectors to search for product {product_id}...")
                
                response = store.s3vectors_client.list_vectors(
                    vectorBucketName=store.bucket_name,
                    indexName=store.index_name
                )
                
                vectors = response.get('vectors', [])
                print(f"📊 Found {len(vectors)} total vectors in database")
                
                # Search for the specific product
                for vector in vectors:
                    metadata = vector.get('metadata', {})
                    if str(metadata.get('product_id', '')) == str(product_id):
                        print(f"✅ Product {product_id} found in list!")
                        print(f"  - Product Name: {metadata.get('product_name', 'Unknown')}")
                        print(f"  - Aisle: {metadata.get('aisle', 'Unknown')}")
                        print(f"  - Department: {metadata.get('department', 'Unknown')}")
                        print(f"  - Vector Key: {vector.get('key', 'Unknown')}")
                        return True
                
                print(f"❌ Product {product_id} not found in vector list")
                return False
                
            except Exception as e2:
                print(f"❌ Error listing vectors: {e2}")
                return False
        
    except Exception as e:
        print(f"❌ Error checking product existence: {e}")
        return False

def list_all_products(limit: int = 10):
    """List all products in the S3Vectors database (limited for performance)"""
    try:
        from vector_store_s3 import get_s3_vector_store
        
        print(f"🔄 Listing up to {limit} products in S3Vectors database...")
        
        # Get the S3Vectors store
        store = get_s3_vector_store()
        if not store:
            print("❌ S3Vectors store not initialized")
            return False
        
        try:
            response = store.s3vectors_client.list_vectors(
                vectorBucketName=store.bucket_name,
                indexName=store.index_name
            )
            
            vectors = response.get('vectors', [])
            print(f"📊 Found {len(vectors)} total vectors in database")
            
            # Show first few products
            for i, vector in enumerate(vectors[:limit]):
                metadata = vector.get('metadata', {})
                print(f"  {i+1}. Product ID: {metadata.get('product_id', 'Unknown')}")
                print(f"     Name: {metadata.get('product_name', 'Unknown')}")
                print(f"     Aisle: {metadata.get('aisle', 'Unknown')}")
                print(f"     Department: {metadata.get('department', 'Unknown')}")
                print()
            
            if len(vectors) > limit:
                print(f"... and {len(vectors) - limit} more products")
            
            return True
            
        except Exception as e:
            print(f"❌ Error listing products: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Error listing products: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Check if a product exists in S3Vectors database')
    parser.add_argument('--product-id', type=int, help='Product ID to check')
    parser.add_argument('--list', action='store_true', help='List all products (limited)')
    parser.add_argument('--limit', type=int, default=10, help='Limit for listing products')
    
    args = parser.parse_args()
    
    if args.list:
        list_all_products(args.limit)
    elif args.product_id:
        check_product_exists(args.product_id)
    else:
        # Default: check for product 45000
        print("🧪 Checking for product 45000 (default)")
        check_product_exists(45000) 