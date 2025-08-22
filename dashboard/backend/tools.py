"""
Main tools orchestrator - imports and provides access to all tools
Creates sync wrappers for async functions to ensure LangChain compatibility
"""
import logging
import asyncio
from langchain.tools import Tool, StructuredTool

# Import all tools from their respective modules
from base_tools import clear_cache
from recommendation_tools_async import (
    recommendation_tool_structured,
    cluster_recommendations_tool_structured
)
from search_tools import (
    search_tool,
    product_details_tool,
    get_product_id_tool
)
from user_tools import (
    user_product_probability_tool_structured,
    similar_users_tool,
    user_info_tool,
    popular_products_tool
)

logger = logging.getLogger(__name__)

def run_async_sync(async_func, *args, **kwargs):
    """Helper function to run async functions synchronously for LangChain compatibility"""
    try:
        # Create a new event loop for the thread if needed
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop in current thread, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're in an async context, we can await directly
            # This is a fallback for when the function is called from async code
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, async_func(*args, **kwargs))
                return future.result()
        else:
            # If no loop is running, we can use asyncio.run
            return asyncio.run(async_func(*args, **kwargs))
    except Exception as e:
        logger.error(f"Error running async function in sync context: {e}")
        # Fallback: try to run in a new event loop
        try:
            return asyncio.run(async_func(*args, **kwargs))
        except Exception as e2:
            logger.error(f"Fallback async execution also failed: {e2}")
            raise e2

# Create enhanced tools function
def create_enhanced_tools(vectorstore, user_id=None):
    """Create enhanced tools with sync wrappers for LangChain compatibility"""
    logger.info("Creating tools with sync wrappers for LangChain compatibility")
    
    def get_user_info_tool(*args, **kwargs) -> str:
        if user_id:
            return f"**Your User ID**: *{user_id}*\n\nYou are currently logged in as user {user_id}. I can provide personalized product recommendations based on your purchase history and preferences."
        else:
            return "**No User ID Set**\n\nPlease enter your user ID in the field above to get personalized recommendations. You can use any user ID like '123' for demonstration purposes."

    # Create a new user info tool with the user_id context
    user_info_tool_with_context = StructuredTool(
        name="get_user_info",
        description="Get current user information and status. No parameters needed.",
        func=get_user_info_tool,
        args_schema=user_info_tool.args_schema
    )

    # Create sync wrappers for async tools to ensure LangChain compatibility
    def sync_recommendation_tool(*args, **kwargs):
        return run_async_sync(recommendation_tool_structured.func, *args, **kwargs)
    
    def sync_cluster_recommendations_tool(*args, **kwargs):
        return run_async_sync(cluster_recommendations_tool_structured.func, *args, **kwargs)
    
    def sync_search_tool(*args, **kwargs):
        return run_async_sync(search_tool.func, *args, **kwargs)
    
    def sync_product_details_tool(*args, **kwargs):
        return run_async_sync(product_details_tool.func, *args, **kwargs)
    
    def sync_get_product_id_tool(*args, **kwargs):
        return run_async_sync(get_product_id_tool.func, *args, **kwargs)
    
    def sync_user_product_probability_tool(*args, **kwargs):
        return run_async_sync(user_product_probability_tool_structured.func, *args, **kwargs)
    
    def sync_similar_users_tool(*args, **kwargs):
        return run_async_sync(similar_users_tool.func, *args, **kwargs)
    
    def sync_popular_products_tool(*args, **kwargs):
        return run_async_sync(popular_products_tool.func, *args, **kwargs)

    # Create sync tool instances
    sync_recommendation_tool_structured = StructuredTool(
        name=recommendation_tool_structured.name,
        description=recommendation_tool_structured.description,
        func=sync_recommendation_tool,
        args_schema=recommendation_tool_structured.args_schema
    )
    
    sync_cluster_recommendations_tool_structured = StructuredTool(
        name=cluster_recommendations_tool_structured.name,
        description=cluster_recommendations_tool_structured.description,
        func=sync_cluster_recommendations_tool,
        args_schema=cluster_recommendations_tool_structured.args_schema
    )
    
    sync_search_tool_structured = Tool(
        name=search_tool.name,
        description=search_tool.description,
        func=sync_search_tool
    )
    
    sync_product_details_tool_structured = Tool(
        name=product_details_tool.name,
        description=product_details_tool.description,
        func=sync_product_details_tool
    )
    
    sync_get_product_id_tool_structured = Tool(
        name=get_product_id_tool.name,
        description=get_product_id_tool.description,
        func=sync_get_product_id_tool
    )
    
    sync_user_product_probability_tool_structured = StructuredTool(
        name=user_product_probability_tool_structured.name,
        description=user_product_probability_tool_structured.description,
        func=sync_user_product_probability_tool,
        args_schema=user_product_probability_tool_structured.args_schema
    )
    
    sync_similar_users_tool_structured = Tool(
        name=similar_users_tool.name,
        description=similar_users_tool.description,
        func=sync_similar_users_tool
    )
    
    sync_popular_products_tool_structured = Tool(
        name=popular_products_tool.name,
        description=popular_products_tool.description,
        func=sync_popular_products_tool
    )

    tools = [
        sync_recommendation_tool_structured,
        sync_user_product_probability_tool_structured,
        sync_cluster_recommendations_tool_structured,
        sync_search_tool_structured,
        sync_product_details_tool_structured,
        sync_similar_users_tool_structured,
        user_info_tool_with_context,
        sync_get_product_id_tool_structured,
        sync_popular_products_tool_structured
    ]
    
    logger.info(f"Created {len(tools)} sync-wrapped async tools for LangChain compatibility")
    return tools

# Export all tools for backward compatibility
__all__ = [
    'create_enhanced_tools',
    'clear_cache',
    'recommendation_tool_structured',
    'cluster_recommendations_tool_structured',
    'search_tool',
    'product_details_tool',
    'get_product_id_tool',
    'user_product_probability_tool_structured',
    'similar_users_tool',
    'user_info_tool',
    'popular_products_tool'
    ] 