import logging
import time
from functools import lru_cache
from langchain.tools import Tool
from ml_model import recommend_for_user, get_similar_users, get_user_product_score, get_popular_products, get_user_product_scores_batch
from vector_store_s3 import get_s3_vector_store
from product_lookup import get_product_by_id, get_all_products, search_products
from cache_manager import analysis_cache
from pydantic import BaseModel
from typing import List, Optional
from langchain.tools import StructuredTool

logger = logging.getLogger(__name__)

# Global cache for vector store operations
_vector_store_cache = {}
_product_lookup_cache = {}
_cache_ttl = 300  # 5 minutes cache TTL

# --- Pydantic Schemas ---
class UserInfoInput(BaseModel):
    pass  # No fields needed
class Recommendation(BaseModel):
    product_name: str
    aisle: str
    department: str
    normalized_score: float

class RecommendationList(BaseModel):
    recommendations: List[Recommendation]

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

class RecommendationInput(BaseModel):
    user_id: str
    count: str = "5"

# --- Structured Tool Implementations ---
def get_recommendations_with_count_structured(input: RecommendationInput = None, **kwargs) -> RecommendationList:
    # Accept both RecommendationInput and dict/kwargs for compatibility
    if input is None and kwargs:
        # Called with keyword arguments (e.g., user_id='1234', count='5')
        user_id = kwargs.get('user_id', '').strip()
        count = kwargs.get('count', '5').strip()
    elif input is not None:
        user_id = input.user_id.strip()
        count = input.count.strip()
    else:
        raise ValueError("User ID is required for recommendations.")
    if not user_id:
        raise ValueError("User ID is required for recommendations.")
    try:
        user_id_int = int(user_id)
    except Exception:
        raise ValueError(f"User ID '{user_id}' is not a valid integer.")
    try:
        top_n = int(count)
        if top_n <= 0:
            top_n = 5
    except Exception:
        top_n = 5
    recs = recommend_for_user(user_id_int, N=top_n, normalize=True)
    if not recs:
        # Fallback to popular products
        recs = get_popular_products(N=top_n, normalize=True)
    product_lookup = get_cached_product_lookup()
    recommendations = []
    for product_id, normalized_score in recs:
        metadata = product_lookup.get(str(product_id), {})
        recommendations.append(Recommendation(
            product_name=metadata.get('product_name', f'Product_{product_id}'),
            aisle=metadata.get('aisle', 'Unknown'),
            department=metadata.get('department', 'Unknown'),
            normalized_score=normalized_score
        ))
    return RecommendationList(recommendations=recommendations)

recommendation_tool_structured = StructuredTool(
    name="get_product_recommendations_structured",
    description="Get personalized product recommendations as a structured list.",
    func=get_recommendations_with_count_structured,
    args_schema=RecommendationInput,
    return_schema=RecommendationList
)

def format_probability_results_markdown(results):
    if not results:
        return "No results found."
    if len(results) == 1:
        r = results[0]
        return (
            f"**User-Product Preference Score Analysis**\n\n"
            f"**Product**: *{r.product_name}* (ID: {r.product_id})\n"
            f"**Normalized Score**: *{r.normalized_score:.3f}*\n"
            f"**Confidence Level**: *{r.confidence}* - {r.description}\n\n"
            f"**Score Interpretation:**\n"
            f"• 0.7-1.0: Very high preference/popularity\n"
            f"• 0.5-0.7: High preference/popularity\n"
            f"• 0.3-0.5: Medium preference/popularity\n"
            f"• 0.1-0.3: Low preference/popularity\n"
            f"• 0.0-0.1: Very low preference/popularity\n"
        )
    else:
        out = ["**Multiple Product Preference Score Analysis**\n"]
        for i, r in enumerate(results, 1):
            out.append(
                f"{i}. **Product**: *{r.product_name}* (ID: {r.product_id})\n"
                f"   - Normalized Score: *{r.normalized_score:.3f}*\n"
                f"   - Confidence Level: *{r.confidence}* - {r.description}\n"
            )
        out.append("**Score Interpretation:**\n" \
            "• 0.7-1.0: Very high preference/popularity\n" \
            "• 0.5-0.7: High preference/popularity\n" \
            "• 0.3-0.5: Medium preference/popularity\n" \
            "• 0.1-0.3: Low preference/popularity\n" \
            "• 0.0-0.1: Very low preference/popularity\n")
        return "".join(out)

def get_user_product_probability_structured(user_id: str = None, product_query: str = None, **kwargs) -> str:
    import re
    # Accept both positional and keyword arguments for compatibility
    if user_id is None and 'user_id' in kwargs:
        user_id = kwargs['user_id']
    if product_query is None and 'product_query' in kwargs:
        product_query = kwargs['product_query']
    user_id = (user_id or '').strip()
    if not user_id:
        raise ValueError("User ID is required for this query.")
    try:
        user_id_int = int(user_id)
    except Exception:
        raise ValueError(f"User ID '{user_id}' is not a valid integer.")
    product_query = (product_query or '').strip()
    if not product_query:
        return format_probability_results_markdown([])
    product_queries = [x for x in re.split(r"[\s,]+", product_query) if x]
    results = []
    # Determine if the query is product ID(s) or name(s)
    is_all_digits = all(pq.isdigit() for pq in product_queries)
    is_single_digit = is_all_digits and len(product_queries) == 1
    is_multi_digit = is_all_digits and len(product_queries) > 1

    if is_single_digit:
        logger.info(f"[user_product_probability_tool_structured] Branch: single product_id | product_queries: {product_queries}")
        pq = product_queries[0]
        product_id = int(pq)
        product_metadata = get_product_by_id(str(product_id))
        results = []
        if product_metadata:
            product_name = product_metadata.get('product_name', f'Product_{product_id}')
            normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
            # Confidence bands
            if normalized_score >= 0.7:
                confidence = "VERY_HIGH"
                description = "Very high preference/popularity"
            elif normalized_score >= 0.5:
                confidence = "HIGH"
                description = "High preference/popularity"
            elif normalized_score >= 0.3:
                confidence = "MEDIUM"
                description = "Medium preference/popularity"
            elif normalized_score >= 0.1:
                confidence = "LOW"
                description = "Low preference/popularity"
            else:
                confidence = "VERY_LOW"
                description = "Very low preference/popularity"
            results.append(UserProductProbability(
                product_name=product_name,
                product_id=str(product_id),
                normalized_score=normalized_score,
                confidence=confidence,
                description=description
            ))
        results.sort(key=lambda r: r.normalized_score, reverse=True)
        return format_probability_results_markdown(results[:5])
    elif is_multi_digit:
        logger.info(f"[user_product_probability_tool_structured] Branch: multiple product_ids | product_queries: {product_queries}")
        results = []
        for pq in product_queries[:5]:
            product_id = int(pq)
            product_metadata = get_product_by_id(str(product_id))
            if not product_metadata:
                continue
            product_name = product_metadata.get('product_name', f'Product_{product_id}')
            normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
            # Confidence bands
            if normalized_score >= 0.7:
                confidence = "VERY_HIGH"
                description = "Very high preference/popularity"
            elif normalized_score >= 0.5:
                confidence = "HIGH"
                description = "High preference/popularity"
            elif normalized_score >= 0.3:
                confidence = "MEDIUM"
                description = "Medium preference/popularity"
            elif normalized_score >= 0.1:
                confidence = "LOW"
                description = "Low preference/popularity"
            else:
                confidence = "VERY_LOW"
                description = "Very low preference/popularity"
            results.append(UserProductProbability(
                product_name=product_name,
                product_id=str(product_id),
                normalized_score=normalized_score,
                confidence=confidence,
                description=description
            ))
        results.sort(key=lambda r: r.normalized_score, reverse=True)
        return format_probability_results_markdown(results[:5])
    else:
        # Branch: name/keyword | product_queries
        import heapq
        import time
        seen_ids = set()
        product_id_to_metadata = {}
        search_start = time.time()
        all_search_results = []
        for pq in product_queries:
            pq_search_start = time.time()
            search_results = search_products(pq, limit=120)
            pq_search_end = time.time()
            logger.info(f"[Timing] Search for '{pq}' found {len(search_results)} products in {pq_search_end - pq_search_start:.3f} seconds.")
            for product_metadata in search_results:
                product_id = product_metadata.get('product_id')
                if not product_id or product_id in seen_ids:
                    continue
                seen_ids.add(product_id)
                product_id_to_metadata[product_id] = product_metadata
                if len(seen_ids) >= 120:
                    break
            if len(seen_ids) >= 120:
                break
        search_end = time.time()
        logger.info(f"[Timing] Total product search time for all queries: {search_end - search_start:.3f} seconds.")

        # Batch score calculation
        score_start = time.time()
        product_ids_int = []
        for pid in seen_ids:
            try:
                product_ids_int.append(int(pid))
            except Exception:
                continue
        batch_scores = get_user_product_scores_batch(user_id_int, product_ids_int, normalize=True)
        score_end = time.time()
        logger.info(f"[Timing] Batch score calculation for {len(product_ids_int)} products took {score_end - score_start:.3f} seconds.")

        # Heap for top N
        heap_start = time.time()
        min_heap = []
        for pid in product_ids_int:
            normalized_score = batch_scores.get(pid, 0.0)
            product_metadata = product_id_to_metadata.get(str(pid), {})
            product_name = product_metadata.get('product_name', str(pid))
            # Confidence bands
            if normalized_score >= 0.7:
                confidence = "VERY_HIGH"
                description = "Very high preference/popularity"
            elif normalized_score >= 0.5:
                confidence = "HIGH"
                description = "High preference/popularity"
            elif normalized_score >= 0.3:
                confidence = "MEDIUM"
                description = "Medium preference/popularity"
            elif normalized_score >= 0.1:
                confidence = "LOW"
                description = "Low preference/popularity"
            else:
                confidence = "VERY_LOW"
                description = "Very low preference/popularity"
            heapq.heappush(min_heap, (normalized_score, pid, product_name, confidence, description))
            if len(min_heap) > 120:
                heapq.heappop(min_heap)
        heap_end = time.time()
        logger.info(f"[Timing] Heap build for top 120 took {heap_end - heap_start:.4f} seconds.")
        # Get the top 5 by normalized_score (descending)
        sort_start = time.time()
        top_results = heapq.nlargest(5, min_heap, key=lambda x: x[0])
        sort_end = time.time()
        logger.info(f"[Timing] Heap sort for top 5 took {sort_end - sort_start:.4f} seconds.")
        results = [UserProductProbability(
            product_name=product_name,
            product_id=str(product_id),
            normalized_score=normalized_score,
            confidence=confidence,
            description=description
        ) for (normalized_score, product_id, product_name, confidence, description) in top_results]
        return format_probability_results_markdown(results)

user_product_probability_tool_structured = StructuredTool(
    name="get_user_product_probability_structured",
    description="""
Get the ALS model normalized preference score for a user and one or more products as a readable markdown string.

USAGE INSTRUCTIONS:
- If the user provides a product ID (e.g., '1234'), use that product ID directly to get the score.
- If the user provides a product name or keyword (e.g., 'Apples', 'Honeycrisp Apples', 'Banana'), ALWAYS perform a broad search for up to 120 products matching the name/keyword, then return the top 5 products by normalized score.
- If the user provides a comma-separated list of product IDs (e.g., '1234, 5678'), score those IDs (up to 5).
- For name/keyword queries, do NOT just use the first product found—always aggregate and sort by score.
- The output will always be a markdown-formatted list of products and their scores, sorted by preference.

This tool is designed to maximize relevance for both specific product and broad category queries.
""",
    func=get_user_product_probability_structured,
    args_schema=UserProductProbabilityInput
)

# --- Legacy tools (for compatibility) ---
# Keep only one version of each tool, prefer StructuredTool if available

def get_cached_vector_store():
    current_time = time.time()
    if 'vector_store' in _vector_store_cache:
        cache_time, store = _vector_store_cache['vector_store']
        if current_time - cache_time < _cache_ttl:
            logger.info("✅ Using cached vector store")
            return store
    logger.info("🔄 Loading fresh vector store...")
    store = get_s3_vector_store()
    if store:
        logger.info(f"✅ Vector store loaded successfully: bucket={store.bucket_name}, index={store.index_name}")
        _vector_store_cache['vector_store'] = (current_time, store)
    else:
        logger.error("❌ Failed to load vector store")
    return store

def get_cached_product_lookup():
    current_time = time.time()
    if 'product_lookup' in _product_lookup_cache:
        cache_time, lookup = _product_lookup_cache['product_lookup']
        if current_time - cache_time < _cache_ttl:
            logger.info(f"✅ Using cached product lookup with {len(lookup)} products")
            return lookup
    logger.info("🔄 Building fresh product lookup...")
    try:
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
        # Clean and parse user_id and count robustly
        if isinstance(user_id, str):
            user_id = user_id.strip().strip("'\"`")
        if isinstance(count, str):
            count = count.strip().strip("'\"`")

        # Handle comma-separated input (e.g., "6677, 7")
        if "," in user_id:
            parts = user_id.split(",")
            if len(parts) >= 2:
                user_id = parts[0].strip()
                count = parts[1].strip()

        # Parse user_id and count as integers
        try:
            user_id_int = int(user_id)
        except Exception:
            return f"Error: User ID '{user_id}' is not a valid integer."

        try:
            top_n = int(count)
            if top_n <= 0:
                top_n = 5
        except Exception:
            top_n = 5

        similar_users = get_similar_users(user_id_int, top_n)
        if not similar_users:
            return f"No similar users found for user {user_id_int}."
        result = f"**Top {top_n} users similar to user {user_id_int}:**\n\n"
        for i, sim_user in enumerate(similar_users, 1):
            result += f"{i}. **User ID**: *{sim_user}*\n"
        return result
    except Exception as e:
        return f"Error getting similar users: {str(e)}"

def get_user_info_tool(query: str = "") -> str:
    """Get current user information"""
    # This function now expects user_id to be passed as an argument or set globally
    # For now, it will return a placeholder if user_id is not provided
    # In a real application, user_id would be available from the context or session
    return "**No User ID Set**\n\nPlease enter your user ID in the field above to get personalized recommendations. You can use any user ID like '123' for demonstration purposes."

def get_user_product_probability_tool(input_str: str) -> str:
    """Get the ALS model normalized score for a specific user-product pair or multiple products"""
    import re
    try:
        # Accept user_id as string from frontend, convert to int for ML model
        input_str = input_str.strip().strip("'\"")
        if "," in input_str:
            parts = input_str.split(",")
            if len(parts) >= 2:
                user_id = parts[0].strip().strip("'\"")
                if not user_id:
                    return "Error: User ID is required for this query. Please enter your User ID in the sidebar."
                raw_identifier = parts[1]
                identifiers = [x for x in re.split(r"[\s]+", raw_identifier) if x]
            else:
                return "Error: Invalid format. Please provide user ID and product ID/name as 'user_id, product_id' or 'user_id, product_name'"
        else:
            return "Error: Invalid format. Please provide user ID and product ID/name as 'user_id, product_id' or 'user_id, product_name'"
        # Always convert user_id to int for ML model
        try:
            user_id_int = int(user_id)
        except Exception:
            return f"Error: User ID '{user_id}' is not a valid integer."
        # Parse the input - expect format like "user_id, product_id(s)" or "user_id, product_name(s)"
        if len(identifiers) == 1:
            product_identifier = identifiers[0]
            if re.fullmatch(r"\d+", product_identifier):
                product_id = int(product_identifier)
                product_metadata = get_product_by_id(str(product_id))
                if not product_metadata:
                    return f"Error: Product ID {product_id} not found in database."
                product_name = product_metadata.get('product_name', f'Product_{product_id}')
                try:
                    user_id_int = int(user_id)
                except Exception:
                    return f"Error: User ID '{user_id}' is not a valid integer."
                normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
                return format_single_score_result(user_id_int, product_name, product_id, normalized_score)
            else:
                # treat as name
                logger.info(f"Searching for multiple products: {product_identifier}")
                search_results = search_products(product_identifier, limit=5)
                if not search_results:
                    return f"Error: Product '{product_identifier}' not found in database. Please try a different product name or use a product ID."
                # Always return all found products (up to 5)
                product_scores = []
                try:
                    user_id_int = int(user_id)
                except Exception:
                    return f"Error: User ID '{user_id}' is not a valid integer."
                for product_metadata in search_results:
                    product_id = product_metadata.get('product_id')
                    product_name = product_metadata.get('product_name', product_identifier)
                    if product_id:
                        normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
                        product_scores.append({
                            'product_id': product_id,
                            'product_name': product_name,
                            'normalized_score': normalized_score
                        })
                if not product_scores:
                    return f"Error: No valid products found for '{product_identifier}'."
                return format_multiple_score_results(user_id, product_identifier, product_scores)
        # If multiple identifiers, handle as a batch
        else:
            product_scores = []
            try:
                user_id_int = int(user_id)
            except Exception:
                return f"Error: User ID '{user_id}' is not a valid integer."
            for product_identifier in identifiers[:5]:  # Limit to 5 products
                if re.fullmatch(r"\d+", product_identifier):
                    product_id = int(product_identifier)
                    product_metadata = get_product_by_id(str(product_id))
                    if not product_metadata:
                        continue
                    product_name = product_metadata.get('product_name', f'Product_{product_id}')
                    normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
                    product_scores.append({
                        'product_id': product_id,
                        'product_name': product_name,
                        'normalized_score': normalized_score
                    })
                else:
                    # treat as name, get top match
                    search_results = search_products(product_identifier, limit=1)
                    if not search_results:
                        continue
                    product_metadata = search_results[0]
                    product_id = product_metadata.get('product_id')
                    product_name = product_metadata.get('product_name', product_identifier)
                    if not product_id:
                        continue
                    normalized_score = get_user_product_score(user_id_int, product_id, normalize=True)
                    product_scores.append({
                        'product_id': product_id,
                        'product_name': product_name,
                        'normalized_score': normalized_score
                    })
            if not product_scores:
                return "Error: No valid products found for the provided IDs or names."
            return format_multiple_score_results(user_id, ' '.join(identifiers[:5]), product_scores)
    except ValueError as e:
        return f"Error: Invalid user ID or product ID format. Please provide valid integers."
    except Exception as e:
        logger.error(f"Error getting user-product score: {e}")
        return f"Error getting user-product score: {str(e)}"

def format_single_score_result(user_id, product_name, product_id, normalized_score):
    """Format single product score result"""
    # Updated confidence level bands
    if normalized_score >= 0.7:
        confidence = "VERY_HIGH"
        description = "Very high preference/popularity"
    elif normalized_score >= 0.5:
        confidence = "HIGH"
        description = "High preference/popularity"
    elif normalized_score >= 0.3:
        confidence = "MEDIUM"
        description = "Medium preference/popularity"
    elif normalized_score >= 0.1:
        confidence = "LOW"
        description = "Low preference/popularity"
    else:
        confidence = "VERY_LOW"
        description = "Very low preference/popularity"
    result = f"**User-Product Preference Score Analysis**\n\n"
    result += f"**User ID**: *{user_id}*\n"
    result += f"**Product**: *{product_name}* (ID: {product_id})\n"
    result += f"**Normalized Score**: *{normalized_score:.3f}*\n"
    result += f"**Confidence Level**: *{confidence}* - {description}\n"
    result += "\n**Score Interpretation:**\n"
    result += "• 0.7-1.0: Very high preference/popularity\n"
    result += "• 0.5-0.7: High preference/popularity\n"
    result += "• 0.3-0.5: Medium preference/popularity\n"
    result += "• 0.1-0.3: Low preference/popularity\n"
    result += "• 0.0-0.1: Very low preference/popularity\n"
    return result

def format_multiple_score_results(user_id, search_term, products):
    """Format multiple product score results"""
    result = f"**Multiple Product Preference Score Analysis**\n\n"
    result += f"**User ID**: *{user_id}*\n"
    result += f"**Search Term**: *{search_term}*\n"
    result += f"**Top {len(products)} Products by Preference Score**:\n\n"
    for i, product in enumerate(products, 1):
        normalized_score = product['normalized_score']
        # Updated confidence level bands
        if normalized_score >= 0.7:
            confidence = "VERY_HIGH"
            description = "Very high preference/popularity"
        elif normalized_score >= 0.5:
            confidence = "HIGH"
            description = "High preference/popularity"
        elif normalized_score >= 0.3:
            confidence = "MEDIUM"
            description = "Medium preference/popularity"
        elif normalized_score >= 0.1:
            confidence = "LOW"
            description = "Low preference/popularity"
        else:
            confidence = "VERY_LOW"
            description = "Very low preference/popularity"
        result += f"**{i}. {product['product_name']}** (ID: {product['product_id']})\n"
        result += f"   - Normalized Score: *{normalized_score:.3f}*\n"
        result += f"   - Confidence Level: *{confidence}* - {description}\n\n"
    result += "**Score Interpretation:**\n"
    result += "• 0.7-1.0: Very high preference/popularity\n"
    result += "• 0.5-0.7: High preference/popularity\n"
    result += "• 0.3-0.5: Medium preference/popularity\n"
    result += "• 0.1-0.3: Low preference/popularity\n"
    result += "• 0.0-0.1: Very low preference/popularity\n"
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

def get_popular_products_tool(count: str = "10") -> str:
    """Get the most popular products using purchase count if available, otherwise ALS model proxy."""
    try:
        # Clean and parse count
        count = count.strip().strip("'\"") if isinstance(count, str) else str(count)
        try:
            top_n = int(count)
            if top_n <= 0:
                top_n = 5
        except Exception:
            top_n = 5

        logger.info(f"Getting {top_n} most popular products")
        popular_products = get_popular_products(N=top_n, normalize=True)
        if not popular_products:
            return "No popular products found."

        # Get product metadata for each product
        product_lookup = get_cached_product_lookup()
        result = f"**Top {len(popular_products)} Most Popular Products:**\n\n"
        for i, (product_id, value) in enumerate(popular_products, 1):
            metadata = product_lookup.get(str(product_id), {})
            value_str = f"**Purchase Count**: *{int(value)}*"
            result += f"{i}. **Product**: *{metadata.get('product_name', f'Product_{product_id}')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*; {value_str}\n\n"
        return result
    except Exception as e:
        logger.error(f"Error getting popular products: {e}")
        return f"Error getting popular products: {str(e)}"

# Create tools
search_tool = Tool(
    name="search_product_database",
    description="""Search the product database for information about products, categories, departments, or general product queries.\n\n
    ALWAYS use this tool when users ask about specific products, categories, or general product information.\n
    Examples: 'Tell me about organic fruits', 'Find dairy products', 'Show me beverages', 'What fruits do you have?'\n\n
    Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*\n\n
    IMPORTANT: The agent should expect and accept outputs in this format, and relay the output as-is to the user. Do not expect a specific number of results, extra summary, or any other formatting. Do not try to parse or reformat the output.\n\n
    Pass the search query as the first parameter and the number of results as the second parameter (e.g., 'frozen meals, 10' for 10 frozen meal products).""",
    func=enhanced_search_tool
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
    func=get_product_details
)

similar_users_tool = Tool(
    name="get_similar_users",
    description="""Get similar users to the given user_id. 
    
    ALWAYS use this tool when users ask for similar users, customers, or user recommendations.
    Examples: "Find users like me", "Who are similar customers?", "Show me similar users to user 123"
    
    Pass the user ID '[USER_ID]' as the first parameter and the number of similar users as the second parameter (e.g., '7' for 7 users).""",
    func=get_similar_users_tool
)

# Only return structured tools and unique, necessary tools
def create_enhanced_tools(vectorstore, user_id=None):
    """Create enhanced tools with better integration and caching"""
    def get_user_info_tool(*args, **kwargs) -> str:
        if user_id:
            return f"**Your User ID**: *{user_id}*\n\nYou are currently logged in as user {user_id}. I can provide personalized product recommendations based on your purchase history and preferences."
        else:
            return "**No User ID Set**\n\nPlease enter your user ID in the field above to get personalized recommendations. You can use any user ID like '123' for demonstration purposes."

    user_info_tool = StructuredTool(
        name="get_user_info",
        description="Get current user information and status. No parameters needed.",
        func=get_user_info_tool,
        args_schema=UserInfoInput
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

    popular_products_tool = Tool(
        name="get_popular_products",
        description="""Get the most popular products in the database based on purchase count (if available) or the ALS model's item factors as a proxy.\n\n
        ALWAYS use this tool when users ask for popular, trending, or best-selling products.\n
        Examples: 'Show me popular products', 'What are the top products?', 'Best sellers'\n\n
        Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*; **Purchase Count**: *[count]*\n
        If purchase count is not available, the output will use **Normalized Score** instead.\n\nIMPORTANT: Pass the number of products to return as a parameter (e.g., '10' for 10 products). 
        Default is 5 if not specified.\n\nThe agent should treat both 'Purchase Count' and 'Normalized Score' as valid popularity metrics and relay whichever is present in the output to the user.""",
        func=get_popular_products_tool
    )

    return [
        recommendation_tool_structured,
        user_product_probability_tool_structured,
        search_tool,
        product_details_tool,
        similar_users_tool,
        user_info_tool,
        get_product_id_tool,
        popular_products_tool
    ] 