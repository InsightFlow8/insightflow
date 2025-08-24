"""
Base schemas and utilities for all tools - Async by default
"""
import logging
import time
import asyncio
from typing import List, Optional
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Global cache for operations
_cache = {}
_cache_ttl = 300  # 5 minutes cache TTL

# Initialize cache when module is imported
import asyncio

# --- Pydantic Schemas ---
class UserInfoInput(BaseModel):
    pass  # No fields needed

class RecommendationInput(BaseModel):
    user_id: str
    count: str = "5"

class ClusterRecommendationInput(BaseModel):
    user_id: str
    count: str = "10"

class Recommendation(BaseModel):
    product_name: str
    aisle: str
    department: str
    normalized_score: float

class RecommendationList(BaseModel):
    recommendations: List[Recommendation]
    cluster_recommendations: List[Recommendation] = []
    user_cluster: Optional[int] = None
    has_cluster_data: bool = False

class ClusterRecommendation(BaseModel):
    segment: int
    product_id: int
    score: float
    rank: int

class ClusterRecommendationList(BaseModel):
    user_id: int
    cluster_id: int
    recommendations: List[ClusterRecommendation]

class UserProductProbabilityInput(BaseModel):
    user_id: str
    product_query: str  # product_id or product_name or list

class UserProductProbability(BaseModel):
    product_name: str
    product_id: str
    normalized_score: float
    confidence: str
    description: str

class UserProductProbabilityList(BaseModel):
    results: List[UserProductProbability]

# --- Async Cache Management ---
async def get_cached_item(key: str):
    """Get item from cache if not expired (async version)"""
    current_time = time.time()
    if key in _cache:
        cache_time, item = _cache[key]
        if current_time - cache_time < _cache_ttl:
            logger.info(f"✅ Using cached {key}")
            return item
    return None

async def set_cached_item(key: str, item):
    """Set item in cache with current timestamp (async version)"""
    current_time = time.time()
    _cache[key] = (current_time, item)
    logger.info(f"✅ Cached {key}")

async def clear_cache():
    """Clear all cached items (async version)"""
    _cache.clear()
    logger.info("🗑️ Cache cleared")

async def ensure_user_seg_analyzer():
    """Ensure user segmentation analyzer is available in cache"""
    analyzer = await get_cached_item('user_seg_analyzer')
    if analyzer is None:
        logger.info("🔄 User segmentation analyzer not in cache, initializing...")
        await initialize_cache()
        analyzer = await get_cached_item('user_seg_analyzer')
    
    if analyzer is None:
        logger.error("❌ Failed to initialize user segmentation analyzer")
        return None
    
    return analyzer

async def get_cache_status():
    """Get the status of cached items"""
    status = {}
    for key in _cache:
        cache_time, item = _cache[key]
        age = time.time() - cache_time
        status[key] = {
            'age_seconds': age,
            'ttl_remaining': max(0, _cache_ttl - age),
            'expired': age >= _cache_ttl
        }
    return status

async def force_cache_refresh():
    """Force refresh of all cached items"""
    logger.info("🔄 Forcing cache refresh...")
    _cache.clear()
    await initialize_cache()
    logger.info("✅ Cache refresh completed")

# --- Utility Functions ---
def is_async_available():
    """Check if async is available"""
    try:
        import asyncio
        return True
    except ImportError:
        return False

# For backward compatibility, provide sync wrappers that run async functions
def get_cached_item_sync(key: str):
    """Sync wrapper for get_cached_item - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return None
            return None
        else:
            # Run the async function in the event loop
            return loop.run_until_complete(get_cached_item(key))
    except Exception:
        return None

def set_cached_item_sync(key: str, item):
    """Sync wrapper for set_cached_item - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return
            return
        else:
            # Run the async function in the event loop
            loop.run_until_complete(set_cached_item(key, item))
    except Exception:
        pass

def clear_cache_sync():
    """Sync wrapper for clear_cache - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return
            return
        else:
            # Run the async function in the event loop
            loop.run_until_complete(clear_cache())
    except Exception:
        pass

def ensure_user_seg_analyzer_sync():
    """Sync wrapper for ensure_user_seg_analyzer - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return None
            return None
        else:
            # Run the async function in the event loop
            return loop.run_until_complete(ensure_user_seg_analyzer())
    except Exception:
        return None

def get_cache_status_sync():
    """Sync wrapper for get_cache_status - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return {}
            return {}
        else:
            # Run the async function in the event loop
            return loop.run_until_complete(get_cache_status())
    except Exception:
        return {}

def force_cache_refresh_sync():
    """Sync wrapper for force_cache_refresh - for backward compatibility only"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, just return
            return
        else:
            # Run the async function in the event loop
            loop.run_until_complete(force_cache_refresh())
    except Exception:
        pass

# Initialize critical components in cache
async def initialize_cache():
    """Initialize critical components in cache"""
    try:
        from user_segmentation_analysis import UserSegmentationAnalyzer
        
        # Initialize user segmentation analyzer
        logger.info("🔄 Initializing user segmentation analyzer...")
        analyzer = UserSegmentationAnalyzer()
        
        if analyzer.is_data_available():
            await set_cached_item('user_seg_analyzer', analyzer)
            logger.info("✅ User segmentation analyzer initialized and cached")
        else:
            logger.warning("⚠️ User segmentation analyzer data not available")
            
    except Exception as e:
        logger.error(f"❌ Failed to initialize user segmentation analyzer: {e}")

# Initialize cache when module is imported
try:
    # Try to initialize cache if we're not in an async context
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        asyncio.run(initialize_cache())
    else:
        # Schedule initialization for later
        loop.create_task(initialize_cache())
except Exception as e:
    logger.warning(f"Could not initialize cache immediately: {e}")
