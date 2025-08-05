import logging
import time
from functools import lru_cache
from langchain.tools import Tool
from ml_model import recommend_for_user, get_similar_users
from vector_store_s3 import get_s3_vector_store
from product_lookup import get_product_by_id, get_all_products, search_products
from cache_manager import analysis_cache

logger = logging.getLogger(__name__)

# Global cache for vector store operations
_vector_store_cache = {}
_product_lookup_cache = {}
_cache_ttl = 300  # 5 minutes cache TTL

def create_enhanced_tools(vectorstore, user_id=None):
    """Create enhanced tools with better integration and caching"""
    
    def get_cached_vector_store():
        """Get cached vector store or load fresh"""
        current_time = time.time()
        
        # Check if we have a fresh cache
        if 'vector_store' in _vector_store_cache:
            cache_time, store = _vector_store_cache['vector_store']
            if current_time - cache_time < _cache_ttl:
                logger.info("✅ Using cached vector store")
                return store
        
        logger.info("🔄 Loading fresh vector store...")
        
        # Load fresh vector store
        store = get_s3_vector_store()
        if store:
            logger.info(f"✅ Vector store loaded successfully: bucket={store.bucket_name}, index={store.index_name}")
            _vector_store_cache['vector_store'] = (current_time, store)
        else:
            logger.error("❌ Failed to load vector store")
        return store
    
    def get_cached_product_lookup():
        """Get cached product lookup or build fresh"""
        current_time = time.time()
        
        # Check if we have a fresh cache
        if 'product_lookup' in _product_lookup_cache:
            cache_time, lookup = _product_lookup_cache['product_lookup']
            if current_time - cache_time < _cache_ttl:
                logger.info(f"✅ Using cached product lookup with {len(lookup)} products")
                return lookup
        
        logger.info("🔄 Building fresh product lookup...")
        
        # Use the new product lookup system instead of S3Vectors metadata
        try:
            from product_lookup import get_all_products
            lookup = get_all_products()
            
            if lookup:
                logger.info(f"✅ Built product lookup with {len(lookup)} products from separate system")
                _product_lookup_cache['product_lookup'] = (current_time, lookup)
                return lookup
            else:
                logger.warning("⚠️ No products found in separate lookup system")
                return {}
                
        except Exception as e:
            logger.error(f"Error building product lookup: {e}")
            return {}
    
    def get_recommendations(user_id: str, top_n: int = 5) -> str:
        """Get product recommendations for a specific user - optimized with caching"""
        try:
            # Clean the user_id string - remove quotes and extra whitespace
            if isinstance(user_id, str):
                user_id = user_id.strip().strip("'\"")
            
            # Convert string to integer
            user_id_int = int(user_id)
            
            # Get recommendations from ML model
            recs = recommend_for_user(user_id_int, N=top_n + 3)
            
            if not recs:
                return f"No product recommendations found for user {user_id_int}. This user may not have enough purchase history for personalized recommendations."
            
            logger.info(f"📊 ML model returned {len(recs)} recommendations for user {user_id_int}")
            
            # Get cached product lookup for efficient processing
            product_lookup = get_cached_product_lookup()
            if not product_lookup:
                logger.error("Product lookup is empty - cannot process recommendations")
                return "Product database not available. Please try again later."
            
            logger.info(f"📦 Product lookup contains {len(product_lookup)} products")
            
            # Process recommendations efficiently
            seen_products = set()
            seen_names = set()
            unique_recs = []
            
            for product_id, score in recs:
                if product_id not in seen_products and len(unique_recs) < top_n:
                    # Get product details from lookup
                    product_metadata = product_lookup.get(str(product_id))
                    if product_metadata:
                        product_name = product_metadata.get('product_name', f'Product_{product_id}')
                        
                        # Check if we already have a product with similar name
                        if product_name not in seen_names:
                            seen_products.add(product_id)
                            seen_names.add(product_name)
                            unique_recs.append((product_id, score, product_metadata))
                            logger.debug(f"✅ Added product {product_id} ({product_name}) to recommendations")
                    else:
                        logger.warning(f"⚠️ Product {product_id} not found in product lookup")
            
            logger.info(f"🎯 Processed {len(unique_recs)} unique recommendations from {len(recs)} total")
            
            if not unique_recs:
                # Try to get some sample products to show what's available
                sample_products = list(product_lookup.keys())[:5]
                logger.warning(f"No unique recommendations found. Sample available products: {sample_products}")
                return f"No unique product recommendations found for user {user_id_int}. This user may not have enough purchase history for personalized recommendations."
            
            # Format results efficiently
            result = f"Here are the top {len(unique_recs)} unique product recommendations for user {user_id_int}:\n\n"
            for i, (product_id, score, metadata) in enumerate(unique_recs, 1):
                result += f"{i}. **Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*; **Score**: *{score:.3f}*\n\n"
            
            return result
        except ValueError:
            return f"Error: User ID '{user_id}' is not a valid integer."
        except KeyError:
            return f"Error: User {user_id} not found in the recommendation system."
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return f"Error getting recommendations: {str(e)}"

    def get_recommendations_with_count(user_id: str, count: str = "5") -> str:
        """Get product recommendations with specified count - improved parsing"""
        try:
            # Clean the input string
            user_id = user_id.strip()
            count = count.strip()
            
            # Handle comma-separated input (e.g., "123, 3" or "123,3")
            if "," in user_id:
                parts = user_id.split(",")
                if len(parts) >= 2:
                    user_id = parts[0].strip()
                    count = parts[1].strip()
                else:
                    # If only one part, treat as user_id
                    user_id = parts[0].strip()
                    count = "5"  # default count
            
            # Clean up any extra whitespace or quotes
            user_id = user_id.strip().strip("'\"")
            count = count.strip().strip("'\"")
            
            # Parse the count parameter
            try:
                top_n = int(count)
                if top_n <= 0:
                    top_n = 5  # default to 5 if invalid
            except ValueError:
                top_n = 5  # default to 5 if parsing fails
            
            logger.info(f"Getting {top_n} recommendations for user {user_id}")
            return get_recommendations(user_id, top_n)
        except Exception as e:
            logger.error(f"Error in get_recommendations_with_count: {e}")
            return f"Error getting recommendations: {str(e)}"

    def enhanced_search_tool(query: str, top_k: str = "5") -> str:
        """Enhanced product search with caching"""
        try:
            # Handle comma-separated input (e.g., "frozen meals, 10")
            if "," in query:
                parts = query.split(",")
                if len(parts) >= 2:
                    search_query = parts[0].strip()
                    count = parts[1].strip()
                    top_k = int(count)
                else:
                    search_query = query
                    top_k = int(top_k)
            else:
                search_query = query
                top_k = int(top_k)

            # Use cached vector store
            s3_store = get_cached_vector_store()
            if s3_store is None:
                return "Vector store not initialized. Please try again later."
            
            results = s3_store.search_similar_products(search_query, top_k)
            
            if not results:
                return f"No products found matching: {search_query}"
            
            result = f"Found {len(results)} products related to '{search_query}':\n\n"
            
            for i, item in enumerate(results, 1):
                metadata = item['metadata']
                # Format exactly as requested
                result += f"{i}. **Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*\n\n"
            
            return result
        except ValueError:
            return f"Error: Invalid count '{top_k}'. Please provide a valid number."
        except Exception as e:
            return f"Error searching products: {str(e)}"
    
    def get_product_details(product_id: str) -> str:
        """Get detailed information about a specific product by ID - optimized with caching"""
        try:
            # Debug logging
            logger.info(f"🔍 get_product_details called with: '{product_id}' (type: {type(product_id)})")
            
            # Clean the product_id string - remove quotes and extra whitespace
            if isinstance(product_id, str):
                original_product_id = product_id
                # Remove all types of quotes and whitespace
                product_id = product_id.strip().strip("'\"`").strip()
                logger.info(f"   Cleaned '{original_product_id}' -> '{product_id}'")
            else:
                logger.warning(f"   Product ID is not a string: {product_id} (type: {type(product_id)})")
                product_id = str(product_id).strip().strip("'\"`").strip()
            
            # Try to parse as integer
            try:
                product_id_int = int(product_id)
                logger.info(f"   Successfully parsed product_id: {product_id_int}")
            except ValueError as ve:
                logger.error(f"   Failed to parse '{product_id}' as integer: {ve}")
                # Try to extract just the number from the string
                import re
                number_match = re.search(r'\d+', product_id)
                if number_match:
                    try:
                        product_id_int = int(number_match.group())
                        logger.info(f"   Extracted number from '{product_id}': {product_id_int}")
                    except ValueError:
                        return f"Error: Product ID '{product_id}' is not a valid integer."
                else:
                    return f"Error: Product ID '{product_id}' is not a valid integer."
            
            # Use the new product lookup system
            product_metadata = get_product_by_id(str(product_id_int))
            if product_metadata:
                result = f"**Product**: *{product_metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{product_metadata.get('aisle', 'Unknown')}*; **Department**: *{product_metadata.get('department', 'Unknown')}*"
                logger.info(f"   Found product {product_id_int}: {product_metadata.get('product_name', 'Unknown')}")
                return result
            else:
                logger.warning(f"   Product {product_id_int} not found in lookup")
                return f"Product ID {product_id_int} not found in database."
                
        except Exception as e:
            logger.error(f"   Unexpected error in get_product_details: {e}")
            return f"Error getting product details: {str(e)}"

    def get_similar_users_tool(user_id: str, count: str = "5") -> str:
        """Get similar users to the given user_id, formatted for markdown output."""
        try:
            # Clean the user_id string - remove quotes and extra whitespace
            if isinstance(user_id, str):
                user_id = user_id.strip().strip("'\"")
            
            # Handle comma-separated input (e.g., "6677, 7")
            if "," in user_id:
                parts = user_id.split(",")
                if len(parts) >= 2:
                    user_id = parts[0].strip()
                    count = parts[1].strip()
            
            # Parse the count parameter
            top_n = int(count)
            similar_users = get_similar_users(user_id, top_n)
            if not similar_users:
                return f"No similar users found for user {user_id}."
            result = f"**Top {top_n} users similar to user {user_id}:**\n\n"
            for i, sim_user in enumerate(similar_users, 1):
                result += f"{i}. **User ID**: *{sim_user}*\n"
            return result
        except ValueError:
            return f"Error: Invalid count '{count}'. Please provide a valid number."
        except KeyError:
            return f"Error: User ID '{user_id}' not found in the system."
        except Exception as e:
            return f"Error getting similar users: {str(e)}"

    def get_user_info_tool(query: str = "") -> str:
        """Get current user information"""
        if user_id:
            return f"**Your User ID**: *{user_id}*\n\nYou are currently logged in as user {user_id}. I can provide personalized product recommendations based on your purchase history and preferences."
        else:
            return "**No User ID Set**\n\nPlease enter your user ID in the field above to get personalized recommendations. You can use any user ID like '123' for demonstration purposes."

    # Create tools
    recommendation_tool = Tool(
        name="get_product_recommendations",
        description=f"""Get personalized unique product recommendations for user {user_id if user_id else '[USER_ID]'}. 
        
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*; **Score**: *[score]*
        
        IMPORTANT: Ensure recommendations are unique and diverse, avoiding duplicate product names.
        Use this when users ask for recommendations, suggestions, or what they should buy. 
        Pass the user ID '{user_id}' as the first parameter and the number of recommendations as the second parameter (e.g., '10' for 10 products).""",
        func=get_recommendations_with_count
    )
    
    search_tool = Tool(
        name="search_product_database",
        description="""Search the product database for information about products, categories, departments, or general product queries. 
        
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
        
        IMPORTANT: Provide diverse and unique product results when possible.
        Use this when users ask about specific products, categories, or general product information.
        Pass the search query as the first parameter and the number of results as the second parameter (e.g., 'frozen meals, 10' for 10 frozen meal products).""",
        func=enhanced_search_tool
    )
    
    product_details_tool = Tool(
        name="get_product_details",
        description="""Get detailed information about a specific product by its ID. 
        
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
        
        IMPORTANT: Pass the product ID as a simple number (e.g., '3' for product ID 3).
        Do not include quotes or extra formatting around the product ID.
        Use this to get product names, departments, and aisles for specific product IDs.""",
        func=get_product_details
    )
    
    similar_users_tool = Tool(
    name="get_similar_users",
    description=f"""Get similar users to the given user_id. 
    
    Pass the user ID '{user_id if user_id else '[USER_ID]'}' as the first parameter and the number of similar users as the second parameter (e.g., '7' for 7 users).
    Use this when users ask for similar users, customers, or user recommendations.""",
    func=get_similar_users_tool
    )

    user_info_tool = Tool(
        name="get_user_info",
        description="""Get current user information and status.
        
        Use this when users ask about their user ID, who they are, or their current login status.
        No parameters needed - returns current user information.""",
        func=get_user_info_tool
    )

    return [recommendation_tool, search_tool, product_details_tool, similar_users_tool, user_info_tool] 