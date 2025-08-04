import os
import pandas as pd
import logging
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
from langchain.schema import Document
from data_loader import load_products_for_vector_store

logger = logging.getLogger(__name__)

# Global variable for vector store
vectorstore = None

def load_products_data():
    """
    Load products data using the backend data loader
    """
    logger.info("Loading products data for vector store...")
    
    try:
        products = load_products_for_vector_store()
        
        # Generate text description, e.g.:
        # "Product name: xxx; Aisle: xxx; Department: xxx"
        def create_doc(row):
            return f"product name：{row['product_name']}；aisle：{row.get('aisle', '')}；department：{row.get('department', '')}"
        
        products["doc"] = products.apply(create_doc, axis=1)
        logger.info(f"Successfully loaded {len(products)} products")
        return products
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def init_vector_store():
    """
    Create a new vector store with product data
    This function assumes the collection doesn't exist or has been deleted
    """
    try:
        products = load_products_data()
        texts = products["doc"].tolist()
        
        logger.info(f"Creating new vector store with {len(texts)} documents")
        
        # Check OpenAI API key
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise Exception("OPENAI_API_KEY environment variable is not set")
        
        # Ensure OpenAIEmbeddings is initialized correctly
        embeddings = OpenAIEmbeddings(api_key=openai_api_key)
        logger.info("OpenAI embeddings initialized successfully")
        
        # Connect to Qdrant service
        qdrant_host = os.getenv("QDRANT_HOST", "qdrant")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        
        logger.info(f"Connecting to Qdrant at {qdrant_host}:{qdrant_port}")
        client = QdrantClient(host=qdrant_host, port=qdrant_port)
        
        collection_name = os.getenv("VECTOR_STORE_COLLECTION", "products_collection")
        logger.info(f"Using collection: {collection_name}")
        
        # Create collection (assumes it doesn't exist)
        vector_size = 1536  # OpenAI embeddings dimension
        client.create_collection(
            collection_name=collection_name,
            vectors_config={"size": vector_size, "distance": "Cosine"}
        )
        logger.info(f"Created collection: {collection_name}")
        
        # Create documents with metadata
        documents = []
        for _, row in products.iterrows():
            metadata = {
                'product_id': row['product_id'],
                'product_name': row['product_name'],
                'aisle': row.get('aisle', ''),
                'department': row.get('department', ''),
                'aisle_id': row['aisle_id'],
                'department_id': row['department_id']
            }
            documents.append(Document(page_content=row['doc'], metadata=metadata))
        
        logger.info(f"Created {len(documents)} documents with metadata")
        
        # Use from_documents method to create vector store
        vector_store = QdrantVectorStore.from_documents(
            documents,
            embeddings,
            url=f"http://{qdrant_host}:{qdrant_port}",
            collection_name=collection_name,
        )
        
        logger.info(f"Successfully created vector store with {len(texts)} documents")
        return vector_store
        
    except Exception as e:
        logger.error(f"Error creating vector store: {e}")
        raise

def load_vector_store():
    """Load vector store from existing collection or create new one"""
    
    try:
        # Check OpenAI API key
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise Exception("OPENAI_API_KEY environment variable is not set")
        
        # Initialize embeddings
        embeddings = OpenAIEmbeddings(api_key=openai_api_key)
        logger.info("OpenAI embeddings initialized successfully")
        
        # Connect to Qdrant and check if collection exists
        qdrant_host = os.getenv("QDRANT_HOST", "qdrant")
        qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        
        logger.info(f"Connecting to Qdrant at {qdrant_host}:{qdrant_port}")
        client = QdrantClient(host=qdrant_host, port=qdrant_port)
        collection_name = os.getenv("VECTOR_STORE_COLLECTION", "products_collection")
        
        try:
            # Check if collection exists and has data
            collection_info = client.get_collection(collection_name=collection_name)
            collection_points = client.count(collection_name=collection_name)
            
            if collection_points.count > 0:
                logger.info(f"✅ Collection {collection_name} exists with {collection_points.count} vectors, using existing vector store")
                
                # Return existing vector store without reprocessing data
                vectorstore = QdrantVectorStore.from_existing_collection(
                    collection_name=collection_name,
                    embedding=embeddings,
                    host=qdrant_host,
                    port=qdrant_port
                )
            else:
                logger.info(f"⚠️ Collection {collection_name} exists but is empty, recreating vector store")
                # Delete empty collection and recreate
                client.delete_collection(collection_name=collection_name)
                vectorstore = init_vector_store()
            
        except Exception as e:
            # Collection doesn't exist, create it
            logger.info(f"🔄 Collection {collection_name} not found, creating new vector store")
            vectorstore = init_vector_store()
        
        return vectorstore
        
    except Exception as e:
        logger.error(f"Error loading vector store: {e}")
        raise

def initialize_vector_store():
    """Initialize the vector store"""
    global vectorstore
    
    try:
        logger.info("📚 Loading vector store...")
        vectorstore = load_vector_store()
        logger.info("✅ Vector store initialization completed successfully.")
        
    except Exception as e:
        logger.error(f"❌ Fatal error initializing vector store: {e}")
        # Don't raise the exception, just log it and continue
        logger.error("Vector store initialization failed, but continuing with startup...")
        vectorstore = None

def get_vector_store():
    """Get the global vector store instance"""
    global vectorstore
    return vectorstore 