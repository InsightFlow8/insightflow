# dashboard/backend/vector_store_s3.py
import os
import pandas as pd
import logging
import boto3
import json
from typing import List, Dict, Any
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from data_loader import load_products_for_vector_store
from product_lookup import build_product_lookup_from_data
import time # Added for retry logic

logger = logging.getLogger(__name__)

# Global variable for vector store
vectorstore = None

class S3VectorsStore:
    """
    Vector store implementation using AWS S3 Vectors
    Based on: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html
    """
    
    def __init__(self, bucket_name: str = None, index_name: str = None):
        self.bucket_name = bucket_name or os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        self.index_name = index_name or os.getenv("S3_VECTORS_INDEX", "products-index")
        
        # Initialize S3Vectors client - this is required
        try:
            self.s3vectors_client = boto3.client('s3vectors')
            logger.info("✅ S3Vectors client initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3Vectors client: {e}")
            raise Exception(f"S3Vectors client initialization failed: {e}")
        
        self.embeddings = None
        
    def initialize_embeddings(self):
        """Initialize OpenAI embeddings"""
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise Exception("OPENAI_API_KEY environment variable is not set")
        
        self.embeddings = OpenAIEmbeddings(api_key=openai_api_key)
        logger.info("OpenAI embeddings initialized successfully")
    
    def create_vector_bucket(self):
        """Create S3 Vector bucket if it doesn't exist"""
        try:
            logger.info(f"🔄 Creating S3 Vector bucket: {self.bucket_name}")
            
            # Check if vector bucket exists using S3Vectors client
            try:
                # Try to get the vector bucket to check if it exists
                response = self.s3vectors_client.get_vector_bucket(
                    vectorBucketName=self.bucket_name
                )
                logger.info(f"✅ S3 Vector bucket {self.bucket_name} already exists")
                return
            except Exception as e:
                # Bucket doesn't exist, create it
                logger.info(f" Creating S3 Vector bucket: {self.bucket_name}")
                try:
                    response = self.s3vectors_client.create_vector_bucket(
                        vectorBucketName=self.bucket_name
                    )
                    logger.info(f"✅ Created S3 Vector bucket: {self.bucket_name}")
                except Exception as create_error:
                    if "ConflictException" in str(create_error) and "already exists" in str(create_error):
                        logger.info(f"✅ S3 Vector bucket {self.bucket_name} already exists (detected during creation)")
                    else:
                        logger.error(f"❌ Failed to create vector bucket {self.bucket_name}: {create_error}")
                        raise Exception(f"Failed to create S3 Vector bucket: {create_error}")
                    
        except Exception as e:
            logger.error(f"❌ Error in bucket creation: {e}")
            raise Exception(f"S3 Vector bucket creation failed: {e}")
    
    def create_vector_index(self):
        """Create vector index in S3 Vectors using the correct API"""
        try:
            logger.info(f"🔄 Creating vector index: {self.index_name}")
            
            # Check if index already exists
            try:
                response = self.s3vectors_client.get_index(
                    vectorBucketName=self.bucket_name,
                    indexName=self.index_name
                )
                logger.info(f"✅ Vector index {self.index_name} already exists")
                return response
            except Exception as describe_error:
                # Index doesn't exist, create it
                logger.info(f" Creating new vector index: {self.index_name}")
                
                try:
                    # Use the correct S3Vectors API method for creating index
                    # This is where we specify dataType, dimension, distanceMetric
                    response = self.s3vectors_client.create_index(
                        vectorBucketName=self.bucket_name,
                        indexName=self.index_name,
                        dataType='float32',
                        dimension=1536,
                        distanceMetric='cosine',
                        metadataConfiguration={
                            'nonFilterableMetadataKeys': [
                                'product_id',
                                'product_name', 
                                'aisle',
                                'department',
                                'aisle_id',
                                'department_id',
                                'content'
                            ]
                        }
                    )
                    
                    logger.info(f"✅ Created vector index: {self.index_name}")
                    return response
                    
                except Exception as create_error:
                    # Check if the error is because index already exists
                    if "ConflictException" in str(create_error) and "already exists" in str(create_error):
                        logger.info(f"✅ Vector index {self.index_name} already exists (detected during creation)")
                        # Try to get the index again to get its details
                        try:
                            response = self.s3vectors_client.get_index(
                                vectorBucketName=self.bucket_name,
                                indexName=self.index_name
                            )
                            return response
                        except Exception as final_error:
                            logger.error(f"Failed to get existing index: {final_error}")
                            raise Exception(f"Failed to access existing index: {final_error}")
                    else:
                        logger.error(f"Failed to create vector index: {create_error}")
                        raise Exception(f"Failed to create S3 Vector index: {create_error}")
            
        except Exception as e:
            logger.error(f"Error creating vector index: {e}")
            raise Exception(f"Failed to create S3 Vector index: {e}")
    

    
    def upload_vectors_to_s3(self, products_data: pd.DataFrame):
        """Upload product embeddings to S3Vectors index"""
        try:
            logger.info(f"🔄 Uploading {len(products_data)} vectors to S3Vectors index")
            
            # Initialize embeddings
            self.initialize_embeddings()
            
            # Prepare texts for batch embedding
            texts = []
            for _, row in products_data.iterrows():
                doc_text = f"product name：{row['product_name']}；aisle：{row.get('aisle', '')}；department：{row.get('department', '')}"
                texts.append(doc_text)
            
            # Generate embeddings in batches
            logger.info("Generating embeddings in batches...")
            all_embeddings = []
            batch_size = 100  # OpenAI allows up to 100 texts per request
            
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i + batch_size]
                logger.info(f"Processing embedding batch {i//batch_size + 1}/{(len(texts) + batch_size - 1)//batch_size}")
                
                # Generate embeddings for the batch
                batch_embeddings = self.embeddings.embed_documents(batch_texts)
                all_embeddings.extend(batch_embeddings)
            
            # Prepare vectors for S3Vectors API
            vectors = []
            for idx, (_, row) in enumerate(products_data.iterrows()):
                # Prepare metadata according to S3Vectors API
                metadata = {
                    'product_id': str(row['product_id']),
                    'product_name': row['product_name'],
                    'aisle': row.get('aisle', ''),
                    'department': row.get('department', ''),
                    'aisle_id': str(row['aisle_id']),
                    'department_id': str(row['department_id']),
                    'content': texts[idx]
                }
                
                # Create vector record in S3Vectors format
                vector_record = {
                    'key': f"{row['product_id']}",
                    'data': {
                        'float32': all_embeddings[idx]
                    },
                    'metadata': metadata
                }
                
                vectors.append(vector_record)
            
            # Upload vectors using S3Vectors API
            upload_batch_size = 50  # Smaller batches for S3Vectors
            max_retries = 3
            
            for i in range(0, len(vectors), upload_batch_size):
                batch = vectors[i:i + upload_batch_size]
                batch_num = i//upload_batch_size + 1
                total_batches = (len(vectors) + upload_batch_size - 1) // upload_batch_size
                
                logger.info(f"Uploading batch {batch_num}/{total_batches} to S3Vectors index")
                
                # Retry logic for each batch
                for retry in range(max_retries):
                    try:
                        # Upload directly to S3Vectors index - no paths needed
                        response = self.s3vectors_client.put_vectors(
                            vectorBucketName=self.bucket_name,
                            indexName=self.index_name,
                            vectors=batch
                        )
                        logger.info(f"✅ Successfully uploaded batch {batch_num} to S3Vectors index")
                        break  # Success, exit retry loop
                        
                    except Exception as e:
                        logger.warning(f"Upload attempt {retry + 1} failed for batch {batch_num}: {e}")
                        if retry < max_retries - 1:
                            time.sleep(2 ** retry)  # Exponential backoff
                        else:
                            logger.error(f"Failed to upload batch {batch_num} after {max_retries} attempts")
                            raise Exception(f"Failed to upload batch {batch_num}: {e}")
            
            logger.info(f"✅ Successfully uploaded {len(vectors)} vectors")
            
        except Exception as e:
            logger.error(f"Error uploading vectors: {e}")
            raise
    
    def search_similar_products(self, query: str, top_k: int = 5):
        """Search for similar products using S3Vectors API"""
        try:
            # Generate query embedding
            query_embedding = self.embeddings.embed_query(query)
            
            # Use S3Vectors API for search
            response = self.s3vectors_client.query_vectors(
                vectorBucketName=self.bucket_name,
                indexName=self.index_name,
                queryVector={"float32": query_embedding},  # Wrap in float32 format
                topK=top_k,
                returnMetadata=True  # Explicitly request metadata
            )
            
            # Convert response to our format
            results = []
            for item in response.get('vectors', []):
                results.append({
                    'score': item.get('score', 0),
                    'metadata': item.get('metadata', {}),
                    'key': item.get('key', '')
                })
            
            logger.info(f"✅ Found {len(results)} similar products using S3Vectors API")
            return results
            
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            raise Exception(f"Failed to search products using S3Vectors: {e}")
    

    

    
    def similarity_search(self, query: str, k: int = 5):
        """LangChain-compatible similarity search method"""
        try:
            results = self.search_similar_products(query, k)
            
            # Convert to LangChain Document format
            documents = []
            for result in results:
                metadata = result['metadata']
                content = metadata.get('content', '')
                
                from langchain.schema import Document
                doc = Document(
                    page_content=content,
                    metadata=metadata
                )
                documents.append(doc)
            
            return documents
            
        except Exception as e:
            logger.error(f"Error in similarity_search: {e}")
            return []
    
    def check_product_exists(self, product_id: int):
        """Check if a specific product exists in the vector database"""
        try:
            logger.info(f"Checking if product {product_id} exists in vector database")
            
            # Method 1: Try to get the specific vector by key
            try:
                response = self.s3vectors_client.get_vectors(
                    vectorBucketName=self.bucket_name,
                    indexName=self.index_name,
                    keys=[f"{product_id}"],
                    returnMetadata=True  # Explicitly request metadata
                )
                
                vectors = response.get('vectors', [])
                if vectors:
                    vector = vectors[0]
                    metadata = vector.get('metadata', {})
                    logger.info(f"✅ Product {product_id} found: {metadata.get('product_name', 'Unknown')}")
                    return {
                        'exists': True,
                        'product_id': product_id,
                        'product_name': metadata.get('product_name', 'Unknown'),
                        'aisle': metadata.get('aisle', 'Unknown'),
                        'department': metadata.get('department', 'Unknown'),
                        'vector_key': vector.get('key', 'Unknown')
                    }
                else:
                    logger.info(f"❌ Product {product_id} not found")
                    return {'exists': False, 'product_id': product_id}
                    
            except Exception as e:
                logger.warning(f"Error checking by key: {e}")
                
                # Method 2: List all vectors and search
                try:
                    response = self.s3vectors_client.list_vectors(
                        vectorBucketName=self.bucket_name,
                        indexName=self.index_name,
                        returnMetadata=True  # Explicitly request metadata
                    )
                    
                    vectors = response.get('vectors', [])
                    logger.info(f"Found {len(vectors)} total vectors in database")
                    
                    # Search for the specific product
                    for vector in vectors:
                        metadata = vector.get('metadata', {})
                        if str(metadata.get('product_id', '')) == str(product_id):
                            logger.info(f"✅ Product {product_id} found in list: {metadata.get('product_name', 'Unknown')}")
                            return {
                                'exists': True,
                                'product_id': product_id,
                                'product_name': metadata.get('product_name', 'Unknown'),
                                'aisle': metadata.get('aisle', 'Unknown'),
                                'department': metadata.get('department', 'Unknown'),
                                'vector_key': vector.get('key', 'Unknown')
                            }
                    
                    logger.info(f"❌ Product {product_id} not found in vector list")
                    return {'exists': False, 'product_id': product_id}
                    
                except Exception as e2:
                    logger.error(f"Error listing vectors: {e2}")
                    return {'exists': False, 'product_id': product_id, 'error': str(e2)}
            
        except Exception as e:
            logger.error(f"Error checking product existence: {e}")
            return {'exists': False, 'product_id': product_id, 'error': str(e)}

def load_products_data():
    """
    Load products data using the backend data loader
    """
    logger.info("Loading products data for vector store...")
    
    try:
        products = load_products_for_vector_store()
        
        # Generate text description
        def create_doc(row):
            return f"product name：{row['product_name']}；aisle：{row.get('aisle', '')}；department：{row.get('department', '')}"
        
        products["doc"] = products.apply(create_doc, axis=1)
        logger.info(f"Successfully loaded {len(products)} products")
        return products
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def init_s3_vector_store():
    """
    Create a new S3 Vectors store with product data
    """
    try:
        # Initialize S3 Vectors store with environment variable
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        logger.info(f" Using S3 bucket: {bucket_name}")
        logger.info(f"🔄 Using index name: {index_name}")
        
        s3_store = S3VectorsStore(bucket_name, index_name)
        
        # Create vector bucket if needed
        s3_store.create_vector_bucket()
        
        # Create vector index
        s3_store.create_vector_index()
        
        # Load and upload product data
        products = load_products_data()
        s3_store.upload_vectors_to_s3(products)
        
        # Build product lookup from the same data
        build_product_lookup_from_data(products)
        
        logger.info(f"✅ Successfully created S3 Vectors store with {len(products)} products")
        logger.info(f"✅ Built separate product lookup with {len(products)} products")
        return s3_store
        
    except Exception as e:
        logger.error(f"Error creating S3 Vectors store: {e}")
        raise

def load_s3_vector_store():
    """Load S3 Vectors store from existing index"""
    try:
        bucket_name = os.getenv("S3_VECTORS_BUCKET", "imba-vector-database")
        index_name = os.getenv("S3_VECTORS_INDEX", "products-index")
        
        logger.info(f"🔄 Loading from S3 bucket: {bucket_name}")
        logger.info(f" Loading from index: {index_name}")
        
        s3_store = S3VectorsStore(bucket_name, index_name)
        
        # First check if the index exists
        try:
            logger.info("🔍 Checking if S3Vectors index exists...")
            response = s3_store.s3vectors_client.get_index(
                vectorBucketName=bucket_name,
                indexName=index_name
            )
            logger.info("✅ S3Vectors index exists")
            
            # Now check if product 45000 exists to determine if data is already loaded
            try:
                logger.info("🔍 Checking if product 45000 exists in vector database...")
                product_check = s3_store.check_product_exists(45000)
                
                if product_check.get('exists', False):
                    logger.info(f"✅ Product 45000 found: {product_check.get('product_name', 'Unknown')}")
                    logger.info("✅ Vector database already contains data, using existing store")
                    s3_store.initialize_embeddings()
                    return s3_store
                else:
                    logger.info("❌ Product 45000 not found in vector database")
                    logger.info("🔄 Index exists but no data, loading product data...")
                    return init_s3_vector_store()
                    
            except Exception as e:
                logger.info(f"⚠️ Error checking product 45000: {e}")
                logger.info("🔄 Index exists but error checking data, loading product data...")
                return init_s3_vector_store()
                
        except Exception as e:
            logger.info(f"⚠️ S3Vectors index does not exist: {e}")
            logger.info("🔄 Creating new store and loading product data...")
            return init_s3_vector_store()
        
    except Exception as e:
        logger.error(f"Error loading S3 Vectors store: {e}")
        raise Exception(f"Failed to load S3 Vectors store: {e}")

def initialize_s3_vector_store():
    """Initialize the S3 Vectors store - no fallback, just report errors"""
    global vectorstore
    
    try:
        logger.info(" Loading S3 Vectors store...")
        
        # Try to load or create S3 Vectors store
        try:
            vectorstore = load_s3_vector_store()
            logger.info("✅ S3 Vectors store initialization completed successfully.")
        except Exception as e:
            logger.error(f"❌ S3 Vectors store failed: {e}")
            logger.error("S3 Vectors store initialization failed. Please check:")
            logger.error("1. AWS credentials and permissions")
            logger.error("2. S3_VECTORS_BUCKET environment variable")
            logger.error("3. S3_VECTORS_INDEX environment variable")
            logger.error("4. Network connectivity to AWS")
            vectorstore = None
            raise Exception(f"S3 Vectors store initialization failed: {e}")
            
    except Exception as e:
        logger.error(f"❌ Fatal error initializing S3 Vectors store: {e}")
        logger.error("S3 Vectors store initialization failed, but continuing with startup...")
        vectorstore = None

def get_s3_vector_store():
    """Get the global S3 Vectors store instance"""
    global vectorstore
    return vectorstore

def search_products_s3(query: str, top_k: int = 5):
    """Search for similar products using S3 Vectors"""
    try:
        store = get_s3_vector_store()
        if store:
            return store.search_similar_products(query, top_k)
        else:
            logger.error("S3 Vectors store not initialized")
            return []
    except Exception as e:
        logger.error(f"Error searching products: {e}")
        return []

def check_product_exists_s3(product_id: int):
    """Check if a specific product exists in S3 Vectors database"""
    try:
        store = get_s3_vector_store()
        if store:
            return store.check_product_exists(product_id)
        else:
            logger.error("S3 Vectors store not initialized")
            return {'exists': False, 'product_id': product_id, 'error': 'Store not initialized'}
    except Exception as e:
        logger.error(f"Error checking product existence: {e}")
        return {'exists': False, 'product_id': product_id, 'error': str(e)}