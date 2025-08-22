import logging
import json
import numpy as np
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from ai_agent import process_chat_query, process_chat_query_async, clear_conversation_history
from vector_store_s3 import initialize_s3_vector_store, get_s3_vector_store, search_products_s3
from ml_model import initialize_ml_model


logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize the S3 Vectors store, ALS model, and tools
    """
    try:
        logger.info("🚀 Starting system initialization...")
        
        # Initialize S3 Vectors store
        logger.info("📚 Initializing S3 Vectors store...")
        try:
            initialize_s3_vector_store()
            logger.info("✅ S3 Vectors store initialized successfully")
        except Exception as e:
            logger.error(f"❌ S3 Vectors store initialization failed: {e}")
            logger.warning("⚠️ Continuing without vector store - chat functionality will be limited")
        
        # Initialize ML model
        logger.info("🤖 Initializing ML model...")
        try:
            initialize_ml_model()
            logger.info("✅ ML model initialized successfully")
        except Exception as e:
            logger.error(f"❌ ML model initialization failed: {e}")
            logger.warning("⚠️ Continuing without ML model - recommendations will be limited")
        
        logger.info("✅ Startup event completed successfully.")
        
    except Exception as e:
        logger.error(f"❌ Fatal error during startup: {e}")
        logger.error("Continuing with startup despite initialization errors...")
    
    yield
    
    # Cleanup on shutdown
    logger.info("🔄 Shutting down...")

# Initialize FastAPI app with lifespan
app = FastAPI(title="Product Recommendation System", lifespan=lifespan)

@app.get("/")
async def root():
    """
    Root endpoint providing API information
    """
    return JSONResponse({
        "message": "Product Recommendation System API",
        "version": "1.0.0",
        "description": "AI-powered product recommendation system with customer segmentation",
        "features": [
            "Personalized product recommendations using ALS collaborative filtering",
            "Customer segmentation based on RFM analysis",
            "Cluster-based product recommendations",
            "AI-powered chat interface for product discovery",
            "Vector-based product search using S3 storage"
        ],
        "endpoints": {
            "health": "/health - System health check",
            "health_user_segmentation": "/health/user_segmentation - User segmentation data health check",
            "chat": "/chat - AI chat with product recommendations",
            "search_products": "/search_products - Product search functionality",
            "clear_history": "/clear_history - Clear conversation history",
            "docs": "/docs - Interactive API documentation (Swagger UI)",
            "redoc": "/redoc - Alternative API documentation"
        },
        "status": "running",
        "documentation": "Visit /docs for interactive API documentation"
    })

@app.get("/api")
async def api_info():
    """
    Detailed API information endpoint
    """
    return JSONResponse({
        "api": {
            "name": "Product Recommendation System",
            "version": "1.0.0",
            "description": "Comprehensive product recommendation API combining ML models and customer segmentation"
        },
        "models": {
            "recommendation_engine": "ALS (Alternating Least Squares) collaborative filtering",
            "customer_segmentation": "RFM-based clustering with K-means",
            "vector_search": "S3-based vector similarity search"
        },
        "endpoints": {
            "POST /chat": {
                "description": "AI-powered chat interface for product recommendations",
                "parameters": ["query", "session_id", "user_id"],
                "returns": "AI response with product recommendations"
            },
            "POST /search_products": {
                "description": "Vector-based product search",
                "parameters": ["query", "top_k"],
                "returns": "List of similar products"
            },
            "GET /health": {
                "description": "System health and initialization status",
                "returns": "System health information"
            }
        }
    })

@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify system initialization
    """
    try:
        vectorstore = get_s3_vector_store()
        if vectorstore is None:
            return JSONResponse({
                "status": "initializing",
                "message": "System is still starting up"
            })
        
        return JSONResponse({
            "status": "healthy",
            "message": "System is ready",
            "vectorstore_initialized": vectorstore is not None,
            "vectorstore_type": "S3_Vectors"
        })
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse({
            "status": "error",
            "message": f"System error: {str(e)}"
        })

@app.get("/health/user_segmentation")
async def user_segmentation_health_check():
    """
    Health check endpoint specifically for user segmentation data
    """
    try:
        from user_segmentation_analysis import UserSegmentationAnalyzer
        
        # Try to initialize the analyzer
        analyzer = UserSegmentationAnalyzer()
        
        # Get data status
        data_status = analyzer.get_data_status()
        
        # Get sample data for debugging
        sample_data = analyzer.get_sample_data(n=2)
        
        if analyzer.is_data_available():
            return JSONResponse({
                "status": "healthy",
                "message": "User segmentation data is available",
                "data_status": data_status,
                "sample_data": sample_data
            })
        else:
            return JSONResponse({
                "status": "warning",
                "message": "User segmentation data is not fully available",
                "data_status": data_status,
                "sample_data": sample_data
            })
            
    except Exception as e:
        logger.error(f"User segmentation health check error: {e}")
        return JSONResponse({
            "status": "error",
            "message": f"User segmentation error: {str(e)}",
            "error_type": type(e).__name__
        })

@app.post("/clear_history")
async def clear_conversation_history_endpoint(request: Request):
    """Clear conversation history for a session"""
    try:
        data = await request.json()
        session_id = data.get("session_id")
        
        if not session_id:
            raise HTTPException(status_code=400, detail="Session ID is required.")
        
        success = clear_conversation_history(session_id)
        
        if success:
            return JSONResponse({
                "message": "Conversation history cleared successfully",
                "session_id": session_id
            })
        else:
            return JSONResponse({
                "message": "Session not found",
                "session_id": session_id
            })
    except Exception as e:
        logger.error(f"Clear history error: {e}")
        return JSONResponse({
            "error": f"Error clearing history: {str(e)}"
        })

@app.post("/chat")
async def hybrid_agent_chat(request: Request):
    """Chat endpoint using hybrid agent with conversation memory"""
    try:
        data = await request.json()
        query = data.get("query")
        session_id = data.get("session_id")
        user_id = data.get("user_id")
        
        logger.info(f"Chat request received - Query: '{query}', User ID: {user_id}, Session ID: {session_id}")
        
        if not query:
            logger.error("No query provided")
            raise HTTPException(status_code=400, detail="Query is required.")

        # Check if S3 Vectors store is initialized
        logger.info("Checking S3 Vectors store initialization...")
        vectorstore = get_s3_vector_store()
        if vectorstore is None:
            logger.warning("S3 Vectors store not initialized")
            return JSONResponse({
                "answer": "System is still initializing. Please try again in a moment.",
                "type": "error",
                "error": "S3 Vectors store not initialized"
            })

        # Process the chat query using the AI agent
        logger.info("Processing chat query with AI agent...")
        result = await process_chat_query_async(query, session_id, user_id)
        
        logger.info(f"AI agent response type: {result.get('type', 'unknown')}")
        logger.info(f"AI agent response: {result.get('answer', 'No answer')[:100]}...")
        
        return JSONResponse(result)
        
    except Exception as e:
        logger.error(f"Chat endpoint error: {e}")
        return JSONResponse({
            "answer": f"I'm sorry, I encountered an error processing your request: {str(e)}",
            "type": "error",
            "error": str(e)
        })

@app.post("/search_products")
async def search_products_endpoint(request: Request):
    """Search for similar products using S3 Vectors"""
    try:
        data = await request.json()
        query = data.get("query")
        top_k = data.get("top_k", 5)
        
        if not query:
            raise HTTPException(status_code=400, detail="Query is required.")
        
        results = search_products_s3(query, top_k)
        
        return JSONResponse({
            "query": query,
            "results": results,
            "count": len(results)
        })
        
    except Exception as e:
        logger.error(f"Product search error: {e}")
        return JSONResponse({
            "error": f"Error searching products: {str(e)}"
        }) 