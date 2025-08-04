import os
import logging
import uvicorn
from routes import app

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import os
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    logger.info(f"🚀 Starting FastAPI server on {host}:{port}")
    logger.info("📝 Available endpoints:")
    logger.info("  - GET  /health")
    logger.info("  - POST /chat")
    logger.info("  - POST /clear_history")
    
    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    except Exception as e:
        logger.error(f"❌ Server startup error: {e}")
        raise 