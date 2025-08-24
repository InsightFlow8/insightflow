#!/usr/bin/env python3
"""
Test script to verify similar users tool functionality
"""
import os
import sys
import logging

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(__file__))

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_similar_users_tool():
    """Test the similar users tool directly"""
    try:
        from user_tools import get_similar_users_tool_async
        import asyncio
        
        logger.info("Testing similar users tool...")
        
        # Test with user ID 555 (from the logs)
        result = asyncio.run(get_similar_users_tool_async("555", "5"))
        
        logger.info("✅ Similar users tool result:")
        print("\n" + "="*50)
        print(result)
        print("="*50)
        
        # Check if the result contains expected information
        if "**Top 5 Users Similar to User 555**" in result:
            logger.info("✅ Result format is correct")
        else:
            logger.warning("⚠️ Result format may be incorrect")
            
        if "User Cluster" in result:
            logger.info("✅ Cluster information is present")
        else:
            logger.warning("⚠️ Cluster information missing")
            
        if "Similar Users" in result:
            logger.info("✅ Similar users list is present")
        else:
            logger.warning("⚠️ Similar users list missing")
            
    except Exception as e:
        logger.error(f"❌ Similar users tool test failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

def test_tools_initialization():
    """Test that tools are properly initialized"""
    try:
        from tools import create_enhanced_tools
        
        logger.info("Testing tools initialization...")
        
        # Create tools (this should initialize the cache)
        tools = create_enhanced_tools(None, user_id="555")
        
        logger.info(f"✅ Created {len(tools)} tools")
        
        # Check if similar users tool is present
        similar_users_tool = None
        for tool in tools:
            if tool.name == "get_similar_users":
                similar_users_tool = tool
                break
        
        if similar_users_tool:
            logger.info("✅ Similar users tool found in tools list")
        else:
            logger.warning("⚠️ Similar users tool not found in tools list")
            
    except Exception as e:
        logger.error(f"❌ Tools initialization test failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    logger.info("Starting similar users tool tests...")
    
    # Test 1: Tools initialization
    test_tools_initialization()
    
    print("\n" + "="*50 + "\n")
    
    # Test 2: Similar users tool functionality
    test_similar_users_tool()
    
    logger.info("Tests completed.")
