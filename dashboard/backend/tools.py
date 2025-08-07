import logging
import time
from functools import lru_cache
from langchain.tools import Tool
from ml_model import recommend_for_user, get_similar_users, get_user_product_score
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
        """Get product recommendations for a user"""
        try:
            # Clean the user_id string - remove quotes and extra whitespace
            if isinstance(user_id, str):
                user_id = user_id.strip().strip("'\"")
            
            # Convert string to integer
            user_id_int = int(user_id)
            
            # Get recommendations from ML model with normalized scores
            recs = recommend_for_user(user_id_int, N=top_n + 3, normalize=True)
            
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
            
            for product_id, normalized_score in recs:
                if product_id not in seen_products and len(unique_recs) < top_n:
                    # Get product details from lookup
                    product_metadata = product_lookup.get(str(product_id))
                    if product_metadata:
                        product_name = product_metadata.get('product_name', f'Product_{product_id}')
                        
                        # Check if we already have a product with similar name
                        if product_name not in seen_names:
                            seen_products.add(product_id)
                            seen_names.add(product_name)
                            unique_recs.append((product_id, normalized_score, product_metadata))
                            logger.debug(f"✅ Added product {product_id} ({product_name}) to recommendations")
                    else:
                        logger.warning(f"⚠️ Product {product_id} not found in product lookup")
            
            logger.info(f"🎯 Processed {len(unique_recs)} unique recommendations from {len(recs)} total")
            
            if not unique_recs:
                # Try to get some sample products to show what's available
                sample_products = list(product_lookup.keys())[:5]
                logger.warning(f"No unique recommendations found. Sample available products: {sample_products}")
                return f"No unique product recommendations found for user {user_id_int}. This user may not have enough purchase history for personalized recommendations."
            
            # Format results efficiently with normalized score percentages
            result = f"Here are the top {len(unique_recs)} unique product recommendations for user {user_id_int}:\n\n"
            for i, (product_id, normalized_score, metadata) in enumerate(unique_recs, 1):
                result += f"{i}. **Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*; **Normalized Score**: *{normalized_score:.3f}*\n\n"
            
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
            s3_store = get_cached_vector_store()
            if s3_store is None:
                return "Vector store not initialized. Please try again later."
            
            results = s3_store.search_similar_products(search_query, top_k_int)
            
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

    def get_user_product_probability_tool(input_str: str) -> str:
        """Get the ALS model normalized score for a specific user-product pair or multiple products"""
        try:
            # Clean the input string
            input_str = input_str.strip().strip("'\"")
            
            # Parse the input - expect format like "user_id, product_id" or "user_id, product_name"
            if "," in input_str:
                parts = input_str.split(",")
                if len(parts) >= 2:
                    user_id = parts[0].strip().strip("'\"")
                    product_identifier = parts[1].strip().strip("'\"")
                else:
                    return "Error: Invalid format. Please provide user ID and product ID/name as 'user_id, product_id' or 'user_id, product_name'"
            else:
                return "Error: Invalid format. Please provide user ID and product ID/name as 'user_id, product_id' or 'user_id, product_name'"
            
            # Check if product_identifier is a number (product ID) or string (product name)
            product_id = None
            product_name = None
            
            try:
                # Try to parse as product ID first
                product_id = int(product_identifier)
                # Get product details for context
                product_metadata = get_product_by_id(str(product_id))
                if product_metadata:
                    product_name = product_metadata.get('product_name', f'Product_{product_id}')
                else:
                    return f"Error: Product ID {product_id} not found in database."
            
                # Single product case
                normalized_score = get_user_product_score(user_id, product_id, normalize=True)
                return format_single_score_result(user_id, product_name, product_id, normalized_score)
            
            except ValueError:
                # If not a number, treat as product name and search for multiple matches
                logger.info(f"Searching for multiple products: {product_identifier}")
                
                # Search for the product by name
                search_results = search_products(product_identifier, limit=10)
                
                if not search_results:
                    return f"Error: Product '{product_identifier}' not found in database. Please try a different product name or use a product ID."
                
                # If only one result, return single score
                if len(search_results) == 1:
                    product_metadata = search_results[0]
                    product_id = product_metadata.get('product_id')
                    product_name = product_metadata.get('product_name', product_identifier)
                    
                    if not product_id:
                        return f"Error: Could not find product ID for '{product_identifier}'"
                    
                    normalized_score = get_user_product_score(user_id, product_id, normalize=True)
                    return format_single_score_result(user_id, product_name, product_id, normalized_score)
                
                # Multiple results - get scores for top 3 most relevant
                logger.info(f"Found {len(search_results)} products matching '{product_identifier}'. Getting top 3 scores.")
                
                # Get scores for all results
                product_scores = []
                for product_metadata in search_results[:5]:  # Check first 5 results
                    product_id = product_metadata.get('product_id')
                    product_name = product_metadata.get('product_name', product_identifier)
                    
                    if product_id:
                        normalized_score = get_user_product_score(user_id, product_id, normalize=True)
                        product_scores.append({
                            'product_id': product_id,
                            'product_name': product_name,
                            'normalized_score': normalized_score
                        })
                
                # Sort by score (highest first) and take top 3
                product_scores.sort(key=lambda x: x['normalized_score'], reverse=True)
                top_3_products = product_scores[:3]
                
                return format_multiple_score_results(user_id, product_identifier, top_3_products)
            
        except ValueError as e:
            return f"Error: Invalid user ID or product ID format. Please provide valid integers."
        except Exception as e:
            logger.error(f"Error getting user-product score: {e}")
            return f"Error getting user-product score: {str(e)}"

    def format_single_score_result(user_id, product_name, product_id, normalized_score):
        """Format single product score result"""
        # Determine confidence level based on normalized score
        if normalized_score >= 0.7:
            confidence = "HIGH"
            description = "Very high preference alignment"
        elif normalized_score >= 0.4:
            confidence = "MEDIUM"
            description = "Good preference alignment"
        elif normalized_score >= 0.1:
            confidence = "LOW"
            description = "Some preference alignment"
        else:
            confidence = "VERY_LOW"
            description = "Low preference alignment"
        
        result = f"**User-Product Preference Score Analysis**\n\n"
        result += f"**User ID**: *{user_id}*\n"
        result += f"**Product**: *{product_name}* (ID: {product_id})\n"
        result += f"**Normalized Score**: *{normalized_score:.3f}*\n"
        result += f"**Confidence Level**: *{confidence}* - {description}\n"
        
        return result

    def format_multiple_score_results(user_id, search_term, products):
        """Format multiple product score results"""
        result = f"**Multiple Product Preference Score Analysis**\n\n"
        result += f"**User ID**: *{user_id}*\n"
        result += f"**Search Term**: *{search_term}*\n"
        result += f"**Top 3 Products by Preference Score**:\n\n"
        
        for i, product in enumerate(products, 1):
            normalized_score = product['normalized_score']
            
            # Determine confidence level
            if normalized_score >= 0.7:
                confidence = "HIGH"
                description = "Very high preference alignment"
            elif normalized_score >= 0.4:
                confidence = "MEDIUM"
                description = "Good preference alignment"
            elif normalized_score >= 0.1:
                confidence = "LOW"
                description = "Some preference alignment"
            else:
                confidence = "VERY_LOW"
                description = "Low preference alignment"
            
            result += f"**{i}. {product['product_name']}** (ID: {product['product_id']})\n"
            result += f"   - Normalized Score: *{normalized_score:.3f}*\n"
            result += f"   - Confidence Level: *{confidence}* - {description}\n\n"
        
        return result

    def get_product_id_by_name_tool(product_name: str) -> str:
        """Get product ID by product name with improved fuzzy matching"""
        try:
            # Clean the product name
            product_name = product_name.strip().strip("'\"")
            
            if not product_name:
                return "Error: Product name is required."
            
            logger.info(f"🔍 Searching for product ID by name: '{product_name}'")
            
            # Get all products for more comprehensive search
            all_products = get_all_products()
            
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

    # Create tools
    recommendation_tool = Tool(
        name="get_product_recommendations",
        description=f"""Get personalized unique product recommendations for user {user_id if user_id else '[USER_ID]'}. 
        
        ALWAYS use this tool when users ask for recommendations, suggestions, or what they should buy.
        Examples: "What should I buy?", "Give me recommendations", "Suggest products for me"
        
        CRITICAL: ALWAYS include the normalized score in the output format.
        Format output EXACTLY as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*; **Normalized Score**: *[score]*
        
        IMPORTANT: 
        - Ensure recommendations are unique and diverse, avoiding duplicate product names
        - ALWAYS include the Normalized Score for each product
        - The normalized score shows preference alignment (0.0 = low preference, 1.0 = high preference)
        Pass the user ID '{user_id}' as the first parameter and the number of recommendations as the second parameter (e.g., '10' for 10 products).""",
        func=get_recommendations_with_count
    )
    
    search_tool = Tool(
        name="search_product_database",
        description="""Search the product database for information about products, categories, departments, or general product queries. 
        
        ALWAYS use this tool when users ask about specific products, categories, or general product information.
        Examples: "Tell me about organic fruits", "Find dairy products", "Show me beverages", "What fruits do you have?"
        
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
        
        IMPORTANT: Provide diverse and unique product results when possible.
        Pass the search query as the first parameter and the number of results as the second parameter (e.g., 'frozen meals, 10' for 10 frozen meal products).""",
        func=enhanced_search_tool
    )
    
    product_details_tool = Tool(
        name="get_product_details",
        description="""Get detailed information about a specific product by its ID. 
        
        ALWAYS use this tool when users ask about specific product IDs or want details about a particular product.
        Examples: "Tell me about product 3", "What is product 45000?", "Show me details for product 123"
        
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
        
        IMPORTANT: Pass the product ID as a simple number (e.g., '3' for product ID 3).
        Do not include quotes or extra formatting around the product ID.
        Use this to get product names, departments, and aisles for specific product IDs.""",
        func=get_product_details
    )
    
    similar_users_tool = Tool(
        name="get_similar_users",
        description=f"""Get similar users to the given user_id. 
        
        ALWAYS use this tool when users ask for similar users, customers, or user recommendations.
        Examples: "Find users like me", "Who are similar customers?", "Show me similar users to user 123"
        
        Pass the user ID '{user_id if user_id else '[USER_ID]'}' as the first parameter and the number of similar users as the second parameter (e.g., '7' for 7 users).""",
        func=get_similar_users_tool
    )

    user_info_tool = Tool(
        name="get_user_info",
        description="""Get current user information and status.
        
        ALWAYS use this tool when users ask about their user ID, who they are, or their current login status.
        Examples: "Who am I?", "What's my user ID?", "Am I logged in?"
        No parameters needed - returns current user information.""",
        func=get_user_info_tool
    )

    user_product_probability_tool = Tool(
        name="get_user_product_probability",
        description="""Get the ALS model normalized preference score for a specific user-product pair. 
    
    This tool can handle:
    1. Single product by ID: "user_id, product_id"
    2. Single product by name: "user_id, product_name" 
    3. Multiple products by name: When multiple products match a name, it returns the top 3 highest preference scores
    
    Examples: 
    - "123, 456" (user 123, product ID 456)
    - "123, Organic Avocado" (user 123, product name)
    - "123, Banana" (user 123, will return top 3 banana products by preference score)
    
    Returns normalized score as percentage and confidence level. Note: These are preference alignment scores, not purchase probabilities.""",
        func=get_user_product_probability_tool
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
        func=get_product_id_by_name_tool
    )

    return [recommendation_tool, search_tool, product_details_tool, similar_users_tool, user_info_tool, user_product_probability_tool, get_product_id_tool] 