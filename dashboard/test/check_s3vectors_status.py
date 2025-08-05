#!/usr/bin/env python3
"""
Script to check the status of S3Vectors buckets and indexes
"""
import os
import boto3

def check_s3vectors_status():
    """Check the status of S3Vectors buckets and indexes"""
    try:
        # Get environment variables
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        print("🔍 S3Vectors Status Check")
        print("=" * 50)
        print(f"Target Bucket: {bucket_name}")
        print(f"Target Index: {index_name}")
        print()
        
        # Create S3Vectors client
        client = boto3.client('s3vectors')
        print("✅ S3Vectors client created successfully")
        print()
        
        # Check available vector buckets
        print("📦 Checking available vector buckets...")
        try:
            response = client.list_vector_buckets()
            buckets = response.get('vectorBuckets', [])
            print(f"Found {len(buckets)} vector buckets:")
            
            for bucket in buckets:
                bucket_name_actual = bucket.get('name', 'Unknown')
                print(f"  - {bucket_name_actual}")
                print(f"    Created: {bucket.get('creationDate', 'Unknown')}")
                print(f"    Region: {bucket.get('region', 'Unknown')}")
                print(f"    Full bucket object: {bucket}")
                print()
                
            # Check if our target bucket exists
            target_bucket_exists = any(bucket.get('name') == bucket_name for bucket in buckets)
            if target_bucket_exists:
                print(f"✅ Target bucket '{bucket_name}' exists")
            else:
                print(f"❌ Target bucket '{bucket_name}' does not exist")
                print("Available buckets:")
                for bucket in buckets:
                    print(f"  - {bucket.get('name', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error listing vector buckets: {e}")
            return False
        
        # If target bucket exists, check indexes
        if target_bucket_exists:
            print(f"\n📋 Checking indexes in bucket '{bucket_name}'...")
            try:
                response = client.list_indexes(vectorBucketName=bucket_name)
                indexes = response.get('indexes', [])
                print(f"Found {len(indexes)} indexes:")
                
                for index in indexes:
                    print(f"  - {index.get('name', 'Unknown')}")
                    print(f"    Created: {index.get('creationDate', 'Unknown')}")
                    print(f"    Dimension: {index.get('dimension', 'Unknown')}")
                    print(f"    Distance Metric: {index.get('distanceMetric', 'Unknown')}")
                    print()
                
                # Check if our target index exists
                target_index_exists = any(index.get('name') == index_name for index in indexes)
                if target_index_exists:
                    print(f"✅ Target index '{index_name}' exists")
                    
                    # Try to get more details about the index
                    try:
                        index_response = client.get_index(
                            vectorBucketName=bucket_name,
                            indexName=index_name
                        )
                        print(f"Index details:")
                        print(f"  - Name: {index_response.get('name', 'Unknown')}")
                        print(f"  - Dimension: {index_response.get('dimension', 'Unknown')}")
                        print(f"  - Distance Metric: {index_response.get('distanceMetric', 'Unknown')}")
                        print(f"  - Data Type: {index_response.get('dataType', 'Unknown')}")
                        print(f"  - Created: {index_response.get('creationDate', 'Unknown')}")
                        
                        # Try to count vectors
                        try:
                            vectors_response = client.list_vectors(
                                vectorBucketName=bucket_name,
                                indexName=index_name
                            )
                            vectors = vectors_response.get('vectors', [])
                            print(f"  - Vector Count: {len(vectors)}")
                            
                            if vectors:
                                print("  - Sample vectors:")
                                for i, vector in enumerate(vectors[:3]):
                                    metadata = vector.get('metadata', {})
                                    print(f"    {i+1}. Product ID: {metadata.get('product_id', 'Unknown')}")
                                    print(f"       Name: {metadata.get('product_name', 'Unknown')}")
                                    print(f"       Key: {vector.get('key', 'Unknown')}")
                                    print()
                            
                        except Exception as e:
                            print(f"  - Error counting vectors: {e}")
                        
                    except Exception as e:
                        print(f"❌ Error getting index details: {e}")
                        
                else:
                    print(f"❌ Target index '{index_name}' does not exist")
                    print("Available indexes:")
                    for index in indexes:
                        print(f"  - {index.get('name', 'Unknown')}")
                        
            except Exception as e:
                print(f"❌ Error listing indexes: {e}")
                return False
        else:
            print(f"\n⚠️  Cannot check indexes because bucket '{bucket_name}' does not exist")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking S3Vectors status: {e}")
        return False

if __name__ == "__main__":
    check_s3vectors_status() 