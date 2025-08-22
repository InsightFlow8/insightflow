"""
Async recommendation tools for product suggestions - Default implementation
"""
import logging
import asyncio
from langchain.tools import StructuredTool
from ml_model import recommend_for_user, get_popular_products
from base_tools import (
    RecommendationInput, ClusterRecommendationInput, Recommendation,
    get_cached_item, set_cached_item
)
from user_segmentation_analysis import UserSegmentationAnalyzer
from product_lookup import get_product_by_id, get_all_products

logger = logging.getLogger(__name__)

async def get_cached_product_lookup():
    """Get cached product lookup"""
    lookup = await get_cached_item('product_lookup')
    if lookup is not None:
        return lookup
    
    logger.info("🔄 Building fresh product lookup...")
    try:
        # Run the blocking operation in a thread pool
        loop = asyncio.get_event_loop()
        lookup = await loop.run_in_executor(None, get_all_products)
        
        if lookup:
            logger.info(f"✅ Built product lookup with {len(lookup)} products from separate system")
            await set_cached_item('product_lookup', lookup)
            return lookup
        else:
            logger.warning("⚠️ No products found in separate lookup system")
            return {}
    except Exception as e:
        logger.error(f"Error building product lookup: {e}")
        return {}

async def get_cached_user_seg_analyzer():
    """Get cached user segmentation analyzer"""
    analyzer = await get_cached_item('user_seg_analyzer')
    if analyzer is not None:
        return analyzer
    
    logger.info("🔄 Loading fresh user segmentation analyzer...")
    try:
        # Run the blocking operation in a thread pool
        loop = asyncio.get_event_loop()
        analyzer = await loop.run_in_executor(None, UserSegmentationAnalyzer)
        
        logger.info("✅ User segmentation analyzer loaded successfully")
        await set_cached_item('user_seg_analyzer', analyzer)
        return analyzer
    except Exception as e:
        logger.error(f"Error loading user segmentation analyzer: {e}")
        return None

async def format_recommendations_consistently(als_recommendations, cluster_recommendations, user_id_int, top_n, user_cluster, has_cluster_data):
    """
    Format recommendations consistently with clear categories
    """
    result = f"**Top {top_n} Product Recommendations for User {user_id_int}**\n\n"
    
    if user_cluster is not None:
        result += f"**User Cluster**: *{user_cluster}*\n\n"
    
    # ALS Recommendations
    if als_recommendations:
        result += "**Personalized Recommendations (Based on Your Purchase History):**\n\n"
        for i, rec in enumerate(als_recommendations, 1):
            result += f"{i}. **Product**: *{rec.product_name}*; **Aisle**: *{rec.aisle}*; **Department**: *{rec.department}*; **Score**: *{rec.normalized_score:.3f}*\n\n"
    
    # Cluster Recommendations
    if cluster_recommendations and has_cluster_data:
        result += "**Cluster-Based Recommendations (Popular in Your Customer Segment):**\n\n"
        for i, rec in enumerate(cluster_recommendations, 1):
            result += f"{i}. **Product**: *{rec.product_name}*; **Aisle**: *{rec.aisle}*; **Department**: *{rec.department}*; **Score**: *{rec.normalized_score:.6f}*\n\n"
    
    result += f"**Note**: Personalized recommendations are based on your individual purchase patterns, while cluster recommendations show products popular among similar customers in your segment."
    
    return result

async def get_recommendations_with_count_structured(input: RecommendationInput = None, **kwargs) -> str:
    """Get personalized product recommendations combining ALS and cluster approaches"""
    # Accept both RecommendationInput and dict/kwargs for compatibility
    if input is None and kwargs:
        user_id = kwargs.get('user_id', '').strip()
        count = kwargs.get('count', '5').strip()
    elif input is not None:
        user_id = input.user_id.strip()
        count = input.count.strip()
    else:
        raise ValueError("User ID is required for recommendations.")
    
    if not user_id:
        raise ValueError("User ID is required for recommendations.")
    
    # Handle special cases and provide helpful error messages
    if user_id.lower() in ['current', 'me', 'user', '']:
        return "Error: Please provide a valid numeric user ID (e.g., '123', '456'). The system needs a specific user ID to provide personalized recommendations."
    
    try:
        user_id_int = int(user_id)
        if user_id_int <= 0:
            return "Error: User ID must be a positive integer. Please provide a valid user ID like '123' or '456'."
    except ValueError:
        return f"Error: User ID '{user_id}' is not a valid integer. Please provide a numeric user ID like '123' or '456'."
    
    try:
        top_n = int(count)
        if top_n <= 0:
            top_n = 5
    except Exception:
        top_n = 5
    
    # Run blocking operations in thread pool
    loop = asyncio.get_event_loop()
    
    # Part 1: Get ALS-based personalized recommendations (async)
    als_recs = await loop.run_in_executor(None, recommend_for_user, user_id_int, top_n, True)
    if not als_recs:
        als_recs = await loop.run_in_executor(None, get_popular_products, top_n, True)
    
    # Part 2: Get product lookup and user segmentation analyzer concurrently
    product_lookup_task = get_cached_product_lookup()
    analyzer_task = get_cached_user_seg_analyzer()
    
    product_lookup, analyzer = await asyncio.gather(product_lookup_task, analyzer_task)
    
    # Process ALS recommendations
    als_recommendations = []
    for product_id, normalized_score in als_recs:
        metadata = product_lookup.get(str(product_id), {})
        als_recommendations.append(Recommendation(
            product_name=metadata.get('product_name', f'Product_{product_id}'),
            aisle=metadata.get('aisle', 'Unknown'),
            department=metadata.get('department', 'Unknown'),
            normalized_score=normalized_score
        ))
    
    # Part 3: Get cluster-based recommendations (async)
    cluster_recommendations = []
    user_cluster = None
    has_cluster_data = False
    
    try:
        if analyzer is not None:
            # Get user cluster and recommendations concurrently
            user_cluster_task = loop.run_in_executor(None, analyzer.get_user_cluster, user_id_int)
            cluster_recs_task = loop.run_in_executor(None, analyzer.get_top_n_recommendations_for_user, user_id_int, top_n)
            
            user_cluster, cluster_recs_df = await asyncio.gather(user_cluster_task, cluster_recs_task)
            
            if user_cluster is not None:
                has_cluster_data = True
                
                if not cluster_recs_df.empty:
                    # Process cluster recommendations
                    for _, row in cluster_recs_df.iterrows():
                        product_id = row['product_id']
                        score = row['score']
                        rank = row['rank']
                        
                        # Get product metadata from cached lookup first
                        metadata = product_lookup.get(str(product_id), {})
                        product_name = metadata.get('product_name', f'Product_{product_id}')
                        aisle = metadata.get('aisle', 'Unknown')
                        department = metadata.get('department', 'Unknown')
                        
                        # Fallback to get_product_details if metadata is missing or Unknown
                        if product_name == f'Product_{product_id}' or aisle == 'Unknown' or department == 'Unknown':
                            try:
                                logger.info(f"Metadata incomplete for product {product_id}, fetching from get_product_details")
                                from search_tools import get_product_details_async
                                product_details = await get_product_details_async(str(product_id))
                                
                                # Simple extraction of metadata from the formatted string
                                if "**Product**: *" in product_details:
                                    start_marker = "**Product**: *"
                                    end_marker = "*"
                                    start_idx = product_details.find(start_marker) + len(start_marker)
                                    end_idx = product_details.find(end_marker, start_idx)
                                    if end_idx > start_idx:
                                        product_name = product_details[start_idx:end_idx]
                                
                                if "**Aisle**: *" in product_details:
                                    start_marker = "**Aisle**: *"
                                    start_idx = product_details.find(start_marker) + len(start_marker)
                                    end_idx = product_details.find("*", start_idx)
                                    if end_idx > start_idx:
                                        aisle = product_details[start_idx:end_idx]
                                
                                if "**Department**: *" in product_details:
                                    start_marker = "**Department**: *"
                                    start_idx = product_details.find(start_marker) + len(start_marker)
                                    end_idx = product_details.find("*", start_idx)
                                    if end_idx > start_idx:
                                        department = product_details[start_idx:end_idx]
                                
                                logger.info(f"Retrieved metadata for product {product_id}: {product_name}, {aisle}, {department}")
                                    
                            except Exception as e:
                                logger.warning(f"Failed to get product details for {product_id}: {e}")
                        
                        cluster_recommendations.append(Recommendation(
                            product_name=product_name,
                            aisle=aisle,
                            department=department,
                            normalized_score=score
                        ))
    except Exception as e:
        logger.warning(f"Could not get cluster recommendations for user {user_id_int}: {e}")
    
    return await format_recommendations_consistently(als_recommendations, cluster_recommendations, user_id_int, top_n, user_cluster, has_cluster_data)

async def get_cluster_recommendations_structured(input: ClusterRecommendationInput = None, **kwargs) -> str:
    """Get top N product recommendations for a user based on their cluster/segment"""
    # Accept both ClusterRecommendationInput and dict/kwargs for compatibility
    if input is None and kwargs:
        user_id = kwargs.get('user_id', '').strip()
        count = kwargs.get('count', '10').strip()
    elif input is not None:
        user_id = input.user_id.strip()
        count = input.count.strip()
    else:
        raise ValueError("User ID is required for cluster recommendations.")
    
    if not user_id:
        raise ValueError("User ID is required for cluster recommendations.")
    
    # Handle special cases and provide helpful error messages
    if user_id.lower() in ['current', 'me', 'user', '']:
        return "Error: Please provide a valid numeric user ID (e.g., '123', '456'). The system needs a specific user ID to provide cluster-based recommendations."
    
    try:
        user_id_int = int(user_id)
        if user_id_int <= 0:
            return "Error: User ID must be a positive integer. Please provide a valid user ID like '123' or '456'."
    except ValueError:
        return f"Error: User ID '{user_id}' is not a valid integer. Please provide a numeric user ID like '123' or '456'."
    
    try:
        top_n = int(count)
        if top_n <= 0:
            top_n = 10
    except Exception:
        top_n = 10
    
    # Get the user segmentation analyzer
    analyzer = await get_cached_user_seg_analyzer()
    if analyzer is None:
        return "Error: User segmentation analyzer not available. Please try again later."
    
    try:
        # Get the user's cluster
        loop = asyncio.get_event_loop()
        cluster_id = await loop.run_in_executor(None, analyzer.get_user_cluster, user_id_int)
        if cluster_id is None:
            return f"User {user_id_int} not found in the segmentation data."
        
        # Get top N recommendations for the user's cluster
        recommendations = await loop.run_in_executor(None, analyzer.get_top_n_recommendations_for_user, user_id_int, top_n)
        
        if recommendations.empty:
            return f"No recommendations found for user {user_id_int} (cluster {cluster_id})."
        
        # Get product metadata for better display
        product_lookup = await get_cached_product_lookup()
        
        # Format the results
        result = f"**Top {len(recommendations)} Product Recommendations for User {user_id_int}**\n\n"
        result += f"**User Cluster**: *{cluster_id}*\n\n"
        
        for i, row in recommendations.iterrows():
            product_id = row['product_id']
            score = row['score']
            rank = row['rank']
            
            # Get product metadata from cached lookup first
            metadata = product_lookup.get(str(product_id), {})
            product_name = metadata.get('product_name', f'Product_{product_id}')
            aisle = metadata.get('aisle', 'Unknown')
            department = metadata.get('department', 'Unknown')
            
            # Fallback to get_product_details if metadata is missing or Unknown
            if product_name == f'Product_{product_id}' or aisle == 'Unknown' or department == 'Unknown':
                try:
                    logger.info(f"Metadata incomplete for product {product_id}, fetching from get_product_details")
                    from search_tools import get_product_details_async
                    product_details = await get_product_details_async(str(product_id))
                    
                    # Simple extraction of metadata from the formatted string
                    if "**Product**: *" in product_details:
                        start_marker = "**Product**: *"
                        end_marker = "*"
                        start_idx = product_details.find(start_marker) + len(start_marker)
                        end_idx = product_details.find(end_marker, start_idx)
                        if end_idx > start_idx:
                            product_name = product_details[start_idx:end_idx]
                    
                    if "**Aisle**: *" in product_details:
                        start_marker = "**Aisle**: *"
                        start_idx = product_details.find(start_marker) + len(start_marker)
                        end_idx = product_details.find("*", start_idx)
                        if end_idx > start_idx:
                            aisle = product_details[start_idx:end_idx]
                    
                    if "**Department**: *" in product_details:
                        start_marker = "**Department**: *"
                        start_idx = product_details.find(start_marker) + len(start_marker)
                        end_idx = product_details.find("*", start_idx)
                        if end_idx > start_idx:
                            department = product_details[start_idx:end_idx]
                    
                    logger.info(f"Retrieved metadata for product {product_id}: {product_name}, {aisle}, {department}")
                        
                except Exception as e:
                    logger.warning(f"Failed to get product details for {product_id}: {e}")
            
            result += f"{i+1}. **Product**: *{product_name}* (ID: {product_id})\n"
            result += f"   - **Aisle**: *{aisle}*\n"
            result += f"   - **Department**: *{department}*\n"
            result += f"   - **Cluster Score**: *{score:.6f}*\n"
            result += f"   - **Rank in Cluster**: *{rank}*\n\n"
        
        result += f"**Note**: These recommendations are based on user {user_id_int}'s cluster ({cluster_id}) and represent the most popular products within that segment."
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting cluster recommendations for user {user_id_int}: {e}")
        return f"Error getting cluster recommendations: {str(e)}"

# Create the structured tools
recommendation_tool_structured = StructuredTool(
    name="get_product_recommendations_structured",
    description="""
Get personalized product recommendations combining two approaches:

1. **Personalized Recommendations**: Uses collaborative filtering to find products similar to what the user has purchased before
2. **Cluster-Based Recommendations**: Uses customer segmentation analysis to find products popular within the user's customer segment

The tool returns a consistently formatted response with both types of recommendations clearly categorized:
- Personalized recommendations based on individual user behavior patterns
- Cluster-based recommendations based on customer segment preferences

This async version provides optimal performance with concurrent processing of database queries, S3 operations, and ML model predictions.

Use this tool when users ask for:
- General product recommendations
- Personalized suggestions
- Product discovery
- Shopping recommendations

This provides the most comprehensive recommendation experience by combining individual preferences with segment-based insights.
""",
    func=get_recommendations_with_count_structured,
    args_schema=RecommendationInput
)

cluster_recommendations_tool_structured = StructuredTool(
    name="get_cluster_recommendations_structured",
    description="""
Get top N product recommendations for a user based on their cluster/segment analysis.

This tool analyzes user segmentation data to:
1. Determine which customer segment/cluster the user belongs to
2. Provide personalized product recommendations based on that cluster's preferences
3. Return detailed product information including aisle, department, and cluster-specific scores

Use this tool when users ask for:
- Personalized recommendations based on their customer segment
- Cluster-based product suggestions
- Segment-specific product popularity

The tool automatically finds the user's cluster and returns the top products for that segment.
""",
    func=get_cluster_recommendations_structured,
    args_schema=ClusterRecommendationInput
)
