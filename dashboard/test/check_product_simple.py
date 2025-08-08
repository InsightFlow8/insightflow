#!/usr/bin/env python3
"""
Simple script to check if a specific product exists in S3Vectors database
"""
import os
import sys
import logging
import boto3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def check_product_exists_simple(product_id: int):
    """Check if a specific product exists in S3Vectors database"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        print(f"🔄 Checking if product {product_id} exists in S3Vectors database...")
        print(f"  Bucket: {bucket_name}")
        print(f"  Index: {index_name}")
        
        # Create S3Vectors client
        client = boto3.client('s3vectors')
        print("✅ S3Vectors client created successfully")
        
        # Method 1: Try to get the specific vector by key
        try:
            print(f"🔄 Method 1: Checking by vector key 'product_{product_id}'...")
            
            # Use get_vectors to check if the specific vector exists
            response = client.get_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name,
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
                
                response = client.list_vectors(
                    vectorBucketName=bucket_name,
                    indexName=index_name
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

def list_products_sample(limit: int = 5):
    """List a sample of products in the S3Vectors database"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        print(f"🔄 Listing up to {limit} products in S3Vectors database...")
        
        # Create S3Vectors client
        client = boto3.client('s3vectors')
        
        try:
            response = client.list_vectors(
                vectorBucketName=bucket_name,
                indexName=index_name
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
    parser.add_argument('--list', action='store_true', help='List sample products')
    parser.add_argument('--limit', type=int, default=5, help='Limit for listing products')
    
    args = parser.parse_args()
    
    if args.list:
        list_products_sample(args.limit)
    elif args.product_id:
        check_product_exists_simple(args.product_id)
    else:
        # Default: check for product 45000
        print("🧪 Checking for product 45000 (default)")
        check_product_exists_simple(45000) 