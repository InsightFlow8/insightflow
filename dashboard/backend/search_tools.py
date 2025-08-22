"""
Search and product lookup tools - Async by default
"""
import logging
import time
import asyncio
from langchain.tools import Tool
from vector_store_s3 import get_s3_vector_store
from product_lookup import get_product_by_id, get_all_products, search_products
from base_tools import get_cached_item, set_cached_item

logger = logging.getLogger(__name__)

async def get_cached_vector_store():
    """Get cached vector store (async version)"""
    lookup = await get_cached_item('vector_store')
    if lookup is not None:
        return lookup
    
    logger.info("🔄 Loading fresh vector store...")
    store = get_s3_vector_store()
    if store:
        logger.info(f"✅ Vector store loaded successfully: bucket={store.bucket_name}, index={store.index_name}")
        await set_cached_item('vector_store', store)
    else:
        logger.error("❌ Failed to load vector store")
    return store

async def enhanced_search_tool_async(query: str, top_k: str = "5") -> str:
    """Enhanced product search with async caching"""
    try:
        # Handle comma-separated input (e.g., "frozen meals, 10")
        if "," in query:
            parts = query.split(",")
            if len(parts) >= 2:
                search_query = parts[0].strip()
                count = parts[1].strip()
                try:
                    top_k_int = int(count)
                except ValueError:
                    top_k_int = 5  # default if parsing fails
            else:
                search_query = query
                try:
                    top_k_int = int(top_k)
                except ValueError:
                    top_k_int = 5  # default if parsing fails
        else:
            search_query = query
            try:
                top_k_int = int(top_k)
            except ValueError:
                top_k_int = 5  # default if parsing fails

        logger.info(f"Searching for '{search_query}' with top_k={top_k_int}")

        # Use cached vector store
        s3_store = await get_cached_vector_store()
        if s3_store is None:
            return "Vector store not initialized. Please try again later."
        
        # Run the search in a thread pool since it's blocking
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, s3_store.search_similar_products, search_query, top_k_int)
        
        if not results:
            return f"No products found matching: {search_query}"
        
        result = f"Found {len(results)} products related to '{search_query}':\n\n"
        
        for i, item in enumerate(results, 1):
            metadata = item['metadata']
            # Format exactly as requested
            result += f"{i}. **Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*\n\n"
        
        return result
    except Exception as e:
        logger.error(f"Error searching products: {e}")
        return f"Error searching products: {str(e)}"

async def get_product_details_async(product_id: str) -> str:
    """Get detailed information about a specific product by ID - async version"""
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
        
        # Use the new product lookup system - run in thread pool since it's blocking
        loop = asyncio.get_event_loop()
        product_metadata = await loop.run_in_executor(None, get_product_by_id, str(product_id_int))
        
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

async def get_product_id_by_name_tool_async(product_name: str) -> str:
    """Get product ID by product name with improved fuzzy matching - async version"""
    try:
        # Clean the product name
        product_name = product_name.strip().strip("'\"")
        
        if not product_name:
            return "Error: Product name is required."
        
        logger.info(f"🔍 Searching for product ID by name: '{product_name}'")
        
        # Get all products for more comprehensive search - run in thread pool since it's blocking
        loop = asyncio.get_event_loop()
        all_products = await loop.run_in_executor(None, get_all_products)
        
        if not all_products:
            return "Error: Product database not available. Please try again later."
        
        # Perform fuzzy search with multiple strategies
        search_results = []
        query_terms = product_name.lower().split()
        
        for product_id, product_info in all_products.items():
            product_name_lower = product_info.get('product_name', '').lower()
            aisle_lower = product_info.get('aisle', '').lower()
            department_lower = product_info.get('department', '').lower()
            
            # Calculate match score
            score = 0
            matched_terms = 0
            
            # Strategy 1: Exact substring matching
            for term in query_terms:
                if (term in product_name_lower or 
                    term in aisle_lower or 
                    term in department_lower):
                    score += 1
                    matched_terms += 1
                
                # Bonus for exact matches
                if term == product_name_lower or term in product_name_lower.split():
                    score += 2
            
            # Only include results with at least one matching term
            if matched_terms > 0:
                search_results.append({
                    'product_id': product_id,
                    'product_name': product_info.get('product_name', 'Unknown'),
                    'aisle': product_info.get('aisle', 'Unknown'),
                    'department': product_info.get('department', 'Unknown'),
                    'score': score
                })
        
        # Sort by score (highest first) and limit results
        search_results.sort(key=lambda x: x['score'], reverse=True)
        search_results = search_results[:10]
        
        if not search_results:
            # Try alternative search strategies
            alternative_results = []
            for product_id, product_info in all_products.items():
                product_name_lower = product_info.get('product_name', '').lower()
                
                # Look for partial matches
                if any(term in product_name_lower for term in query_terms):
                    alternative_results.append({
                        'product_id': product_id,
                        'product_name': product_info.get('product_name', 'Unknown'),
                        'aisle': product_info.get('aisle', 'Unknown'),
                        'department': product_info.get('department', 'Unknown'),
                        'score': 0.5
                    })
            
            if alternative_results:
                search_results = alternative_results[:5]
            else:
                return f"Error: Product '{product_name}' not found in database. Please try a different product name or check the spelling."
        
        # Format the results
        result = f"**Product Search Results for '{product_name}'**\n\n"
        
        for i, product in enumerate(search_results[:5], 1):  # Show top 5 results
            product_id = product.get('product_id', 'Unknown')
            product_name_result = product.get('product_name', 'Unknown')
            aisle = product.get('aisle', 'Unknown')
            department = product.get('department', 'Unknown')
            score = product.get('score', 0)
            
            result += f"{i}. **Product ID**: *{product_id}*; **Product**: *{product_name_result}*; **Aisle**: *{aisle}*; **Department**: *{department}*\n\n"
        
        if len(search_results) > 5:
            result += f"... and {len(search_results) - 5} more results found.\n\n"
        
        # Add helpful note with specific guidance
        if len(search_results) > 1:
            result += "**Note**: Multiple products found. For purchase probability queries, use the Product ID of the most relevant product (usually the first one). If you need a specific type of banana (e.g., fresh bananas), try a more specific search term."
        else:
            result += "**Note**: Use the Product ID with other tools like get_user_product_probability."
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching for product by name: {e}")
        return f"Error searching for product by name: {str(e)}"

# Create the tools with async functions
search_tool = Tool(
    name="search_product_database",
    description="""Search the product database for information about products, categories, departments, or general product queries.\n\n
    ALWAYS use this tool when users ask about specific products, categories, or general product information.\n
    Examples: 'Tell me about organic fruits', 'Find dairy products', 'Show me beverages', 'What fruits do you have?'\n\n
    Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*\n\n
    IMPORTANT: The agent should expect and accept outputs in this format, and relay the output as-is to the user. Do not expect a specific number of results, extra summary, or any other formatting. Do not try to parse or reformat the output.\n\n
    Pass the search query as the first parameter and the number of results as the second parameter (e.g., 'frozen meals, 10' for 10 frozen meal products).""",
    func=enhanced_search_tool_async
)

product_details_tool = Tool(
    name="get_product_details",
    description="""Get detailed information about a specific product by its ID. 
    
    ALWAYS use this tool when users ask about specific product IDs or want details about a particular product.
    Examples: "Tell me about product 3", "What is product 4016?", "Show me details for product 123"
    
    Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
    
    IMPORTANT: Pass the product ID as a simple number (e.g., '3' for product ID 3).
    Do not include quotes or extra formatting around the product ID.
    Use this to get product names, departments, and aisles for specific product IDs.""",
    func=get_product_details_async
)

get_product_id_tool = Tool(
    name="get_product_id_by_name",
    description="""Get product ID by product name with fuzzy matching.
    
    ALWAYS use this tool when users ask about specific product names or when you need to find the product ID for a product name.
    Examples: "What's the product ID for Organic Avocado?", "Find the ID for product XYZ", "Get product ID for bananas", "Find berries blueberries"
    
    Format output as: **Product ID**: *[id]*; **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
    
    IMPORTANT: 
    - Pass the product name as a string (e.g., 'Organic Avocado' or 'bananas' or 'berries')
    - This tool uses fuzzy matching, so it will find products even with partial matches
    - Use this tool before using get_user_product_probability when you only have a product name
    - If the exact product isn't found, try a more specific search term""",
    func=get_product_id_by_name_tool_async
)
