import logging
import json
import numpy as np
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from ai_agent import process_chat_query, clear_conversation_history
from vector_store_s3 import initialize_s3_vector_store, get_s3_vector_store, search_products_s3
from ml_model import initialize_ml_model
from data_loader import get_all_data_for_frontend
from analysis import create_product_affinity_simple, create_customer_journey_flow, create_lifetime_value_analysis, create_churn_analysis
import pandas as pd



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

@app.get("/data")
async def get_data():
    """Get all data for frontend"""
    try:
        df, orders, products, departments, aisles = get_all_data_for_frontend()
        
        # Simple data cleaning - just fill NaN with 0
        def clean_dataframe(df):
            """Clean DataFrame for JSON serialization"""
            return df.fillna(0)
        
        # Clean all DataFrames
        logger.info("Cleaning DataFrames for JSON serialization...")
        
        df = clean_dataframe(df)
        orders = clean_dataframe(orders)
        products = clean_dataframe(products)
        departments = clean_dataframe(departments)
        aisles = clean_dataframe(aisles)
        
        # Convert DataFrames to dict for JSON serialization
        logger.info("Converting DataFrames to JSON...")
        
        df_dict = df.to_dict(orient='records')
        orders_dict = orders.to_dict(orient='records')
        products_dict = products.to_dict(orient='records')
        departments_dict = departments.to_dict(orient='records')
        aisles_dict = aisles.to_dict(orient='records')
        
        logger.info("✅ All DataFrames converted successfully")
        
        result = {
            "df": df_dict,
            "orders": orders_dict,
            "products": products_dict,
            "departments": departments_dict,
            "aisles": aisles_dict
        }
        
        logger.info("✅ Data successfully prepared for frontend")
        return JSONResponse(result)
        
    except Exception as e:
        logger.error(f"Data endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analysis/{analysis_type}")
async def get_analysis(analysis_type: str, request: Request):
    """Get cached analysis results"""
    try:
        data = await request.json()
        df, _, _, _, _ = get_all_data_for_frontend()
        
        if analysis_type == "product_affinity":
            top_products = data.get('top_products', 20)
            result = create_product_affinity_simple(df, top_products)
            return JSONResponse({
                "result": {
                    "pair_counts": dict(result[0]),
                    "product_names": result[1].to_dict(),
                    "product_counts": result[2].to_dict()
                }
            })
        elif analysis_type == "customer_journey":
            result = create_customer_journey_flow(df)
            return JSONResponse({"result": result.to_dict()})
        elif analysis_type == "lifetime_value":
            result = create_lifetime_value_analysis(df)
            return JSONResponse({
                "result": {
                    "figure": result[0].to_dict(),
                    "customer_metrics": result[1].to_dict(orient='records')
                }
            })
        elif analysis_type == "churn":
            result = create_churn_analysis(df)
            return JSONResponse({
                "result": {
                    "figure": result[0].to_dict(),
                    "churn_indicators": result[1].to_dict(orient='records')
                }
            })
        else:
            raise HTTPException(status_code=400, detail="Invalid analysis type")
        
    except Exception as e:
        logger.error(f"Analysis endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        result = process_chat_query(query, session_id, user_id)
        
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