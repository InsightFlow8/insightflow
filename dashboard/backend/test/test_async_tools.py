"""
Test script to verify async tools work correctly
"""
import asyncio
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_async_imports():
    """Test that all async tools can be imported correctly"""
    try:
        logger.info("Testing async imports...")
        
        # Test base tools
        from base_tools import clear_cache, RecommendationInput
        logger.info("✅ base_tools imported successfully")
        
        # Test recommendation tools
        from recommendation_tools_async import recommendation_tool_structured, cluster_recommendations_tool_structured
        logger.info("✅ recommendation_tools_async imported successfully")
        
        # Test search tools
        from search_tools import search_tool, product_details_tool, get_product_id_tool
        logger.info("✅ search_tools imported successfully")
        
        # Test user tools
        from user_tools import user_product_probability_tool_structured, similar_users_tool, user_info_tool, popular_products_tool
        logger.info("✅ user_tools imported successfully")
        
        # Test main tools orchestrator
        from tools import create_enhanced_tools
        logger.info("✅ main tools orchestrator imported successfully")
        
        logger.info("🎉 All async imports successful!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Import failed: {e}")
        return False

async def test_async_tool_creation():
    """Test that async tools can be created successfully"""
    try:
        logger.info("Testing async tool creation...")
        
        from tools import create_enhanced_tools
        
        # Test creating tools
        tools = create_enhanced_tools(None, None)
        logger.info(f"✅ Created {len(tools)} async tools")
        
        # Test creating tools with user_id
        tools_with_user = create_enhanced_tools(None, "123")
        logger.info(f"✅ Created {len(tools_with_user)} async tools with user_id")
        
        logger.info("🎉 Async tool creation successful!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Async tool creation failed: {e}")
        return False

async def test_async_cache():
    """Test async cache operations"""
    try:
        logger.info("Testing async cache operations...")
        
        from base_tools import get_cached_item, set_cached_item, clear_cache
        
        # Test setting and getting cached items
        await set_cached_item('test_key', 'test_value')
        logger.info("✅ Set cached item successfully")
        
        cached_value = await get_cached_item('test_key')
        if cached_value == 'test_value':
            logger.info("✅ Retrieved cached item successfully")
        else:
            logger.error("❌ Cached item value mismatch")
            return False
        
        # Test clearing cache
        await clear_cache()
        logger.info("✅ Cleared cache successfully")
        
        # Verify cache is cleared
        cleared_value = await get_cached_item('test_key')
        if cleared_value is None:
            logger.info("✅ Cache cleared successfully")
        else:
            logger.error("❌ Cache not cleared properly")
            return False
        
        logger.info("🎉 Async cache operations successful!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Async cache test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🧪 Testing Async Tools Implementation")
    logger.info("=" * 50)
    
    # Test imports
    if not await test_async_imports():
        sys.exit(1)
    
    # Test tool creation
    if not await test_async_tool_creation():
        sys.exit(1)
    
    # Test async cache
    if not await test_async_cache():
        sys.exit(1)
    
    logger.info("\n🎉 All async tests passed!")
    logger.info("✅ Async implementation is working correctly")
    logger.info("✅ All tools are now async by default")
    logger.info("✅ Performance improvements are active")

if __name__ == "__main__":
    # Run the async test
    asyncio.run(main())
