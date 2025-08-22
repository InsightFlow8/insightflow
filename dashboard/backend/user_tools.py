"""
User-related tools for user information and similar users - Async by default
"""
import logging
import asyncio
from langchain.tools import Tool, StructuredTool
from ml_model import get_similar_users, get_user_product_score, get_user_product_scores_batch, get_popular_products
from product_lookup import get_product_by_id, search_products
from base_tools import UserInfoInput, UserProductProbabilityInput, UserProductProbability, UserProductProbabilityList

logger = logging.getLogger(__name__)

async def get_cached_product_lookup_async():
    """Get cached product lookup (async version)"""
    from base_tools import get_cached_item, set_cached_item
    
    lookup = await get_cached_item('product_lookup')
    if lookup is not None:
        return lookup
    
    logger.info("🔄 Building fresh product lookup (async)...")
    try:
        # Run the blocking operation in a thread pool
        loop = asyncio.get_event_loop()
        from product_lookup import get_all_products
        lookup = await loop.run_in_executor(None, get_all_products)
        
        if lookup:
            logger.info(f"✅ Built product lookup with {len(lookup)} products from separate system (async)")
            await set_cached_item('product_lookup', lookup)
            return lookup
        else:
            logger.warning("⚠️ No products found in separate lookup system")
            return {}
    except Exception as e:
        logger.error(f"Error building product lookup: {e}")
        return {}

def format_probability_results_markdown(results):
    """Format probability results as markdown"""
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

async def get_user_product_probability_structured_async(user_id: str = None, product_query: str = None, **kwargs) -> str:
    """Get the ALS model normalized preference score for a user and one or more products (async version)"""
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

    # Get event loop for running blocking operations
    loop = asyncio.get_event_loop()

    if is_single_digit:
        logger.info(f"[user_product_probability_tool_structured] Branch: single product_id | product_queries: {product_queries}")
        pq = product_queries[0]
        product_id = int(pq)
        product_metadata = await loop.run_in_executor(None, get_product_by_id, str(product_id))
        results = []
        if product_metadata:
            product_name = product_metadata.get('product_name', f'Product_{product_id}')
            normalized_score = await loop.run_in_executor(None, get_user_product_score, user_id_int, product_id, True)
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
            product_metadata = await loop.run_in_executor(None, get_product_by_id, str(product_id))
            if not product_metadata:
                continue
            product_name = product_metadata.get('product_name', f'Product_{product_id}')
            normalized_score = await loop.run_in_executor(None, get_user_product_score, user_id_int, product_id, True)
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
            search_results = await loop.run_in_executor(None, search_products, pq, 120)
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
        batch_scores = await loop.run_in_executor(None, get_user_product_scores_batch, user_id_int, product_ids_int, True)
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

async def get_similar_users_tool_async(user_id: str, count: str = "5") -> str:
    """Get similar users to the given user_id, formatted for markdown output (async version)"""
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

        # Run the blocking operation in a thread pool
        loop = asyncio.get_event_loop()
        similar_users = await loop.run_in_executor(None, get_similar_users, user_id_int, top_n)
        
        if not similar_users:
            return f"No similar users found for user {user_id_int}."
        result = f"**Top {top_n} users similar to user {user_id_int}:**\n\n"
        for i, sim_user in enumerate(similar_users, 1):
            result += f"{i}. **User ID**: *{sim_user}*\n"
        return result
    except Exception as e:
        return f"Error getting similar users: {str(e)}"

def get_user_info_tool(*args, **kwargs) -> str:
    """Get current user information"""
    # This function now expects user_id to be passed as an argument or set globally
    # For now, it will return a placeholder if user_id is not provided
    # In a real application, user_id would be available from the context or session
    return "**No User ID Set**\n\nPlease enter your user ID in the field above to get personalized recommendations. You can use any user ID like '123' for demonstration purposes."

async def get_popular_products_tool_async(count: str = "10") -> str:
    """Get the most popular products using purchase count if available, otherwise ALS model proxy (async version)"""
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
        
        # Run the blocking operation in a thread pool
        loop = asyncio.get_event_loop()
        popular_products = await loop.run_in_executor(None, get_popular_products, top_n, True)
        
        if not popular_products:
            return "No popular products found."

        # Get product metadata for each product
        product_lookup = await get_cached_product_lookup_async()
        
        result = f"**Top {len(popular_products)} Most Popular Products:**\n\n"
        for i, (product_id, value) in enumerate(popular_products, 1):
            metadata = product_lookup.get(str(product_id), {}) if product_lookup else {}
            value_str = f"**Purchase Count**: *{int(value)}*"
            result += f"{i}. **Product**: *{metadata.get('product_name', f'Product_{product_id}')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*; {value_str}\n\n"
        return result
    except Exception as e:
        logger.error(f"Error getting popular products: {e}")
        return f"Error getting popular products: {str(e)}"

# Create the tools with async functions
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
    func=get_user_product_probability_structured_async,
    args_schema=UserProductProbabilityInput
)

similar_users_tool = Tool(
    name="get_similar_users",
    description="""Get similar users to the given user_id. 
    
    ALWAYS use this tool when users ask for similar users, customers, or user recommendations.
    Examples: "Find users like me", "Who are similar customers?", "Show me similar users to user 123"
    
    Pass the user ID '[USER_ID]' as the first parameter and the number of similar users as the second parameter (e.g., '7' for 7 users).""",
    func=get_similar_users_tool_async
)

user_info_tool = StructuredTool(
    name="get_user_info",
    description="Get current user information and status. No parameters needed.",
    func=get_user_info_tool,
    args_schema=UserInfoInput
)

popular_products_tool = Tool(
    name="get_popular_products",
    description="""Get the most popular products in the database based on purchase count (if available) or the ALS model's item factors as a proxy.\n\n
    ALWAYS use this tool when users ask for popular, trending, or best-selling products.\n
    Examples: 'Show me popular products', 'What are the top products?', 'Best sellers'\n\n
    Format output as: **Product**: *[name]*; **Aisle**: *[aisle]*; **Department**: *[department]*; **Purchase Count**: *[count]*\n
    If purchase count is not available, the output will use **Normalized Score** instead.\n\nIMPORTANT: Pass the number of products to return as a parameter (e.g., '10' for 10 products). 
    Default is 5 if not specified.\n\nThe agent should treat both 'Purchase Count' and 'Normalized Score' as valid popularity metrics and relay whichever is present in the output to the user.""",
    func=get_popular_products_tool_async
)
