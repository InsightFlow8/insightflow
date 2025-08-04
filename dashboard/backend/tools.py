import logging
from langchain.tools import Tool
from ml_model import recommend_for_user, get_similar_users
from vector_store import get_vector_store

logger = logging.getLogger(__name__)

def create_enhanced_tools(vectorstore, user_id=None):
    """Create enhanced tools with better integration"""
    
    def get_recommendations(user_id: str, top_n: int = 5) -> str:
        """Get product recommendations for a specific user - returns product details"""
        try:
            # Clean the user_id string - remove quotes and extra whitespace
            if isinstance(user_id, str):
                user_id = user_id.strip().strip("'\"")
            
            # Convert string to integer
            user_id_int = int(user_id)
            
            # Get more recommendations than requested to ensure we have enough unique products
            recs = recommend_for_user(user_id_int, N=top_n * 3)
            
            # Remove duplicates by product name and get unique products
            seen_products = set()
            seen_names = set()
            unique_recs = []
            
            for product_id, score in recs:
                if product_id not in seen_products and len(unique_recs) < top_n:
                    # Get product details to check name
                    product_details = get_product_details(str(product_id))
                    # Extract product name from the details
                    if "Product: **" in product_details:
                        product_name = product_details.split("Product: **")[1].split("**")[0]
                    else:
                        product_name = f"Product_{product_id}"
                    
                    # Check if we already have a product with similar name
                    if product_name not in seen_names:
                        seen_products.add(product_id)
                        seen_names.add(product_name)
                        unique_recs.append((product_id, score))
            
            if not unique_recs:
                return f"No unique product recommendations found for user {user_id_int}."
            
            # Get product details for each recommendation
            result = f"Here are the top {len(unique_recs)} unique product recommendations for user {user_id_int}:\n\n"
            for i, (product_id, score) in enumerate(unique_recs, 1):
                # Get product details
                product_details = get_product_details(str(product_id))
                result += f"{i}. {product_details}; **Score**: *{score:.3f}*\n\n"
            
            return result
        except ValueError:
            return f"Error: User ID '{user_id}' is not a valid integer."
        except KeyError:
            return f"Error: User {user_id} not found in the recommendation system."
        except Exception as e:
            return f"Error getting recommendations: {str(e)}"

    def get_recommendations_with_count(user_id: str, count: str = "5") -> str:
        """Get product recommendations with specified count"""
        try:
            # Handle comma-separated input (e.g., "1256, 6")
            if "," in user_id:
                parts = user_id.split(",")
                if len(parts) >= 2:
                    user_id = parts[0].strip()
                    count = parts[1].strip()
            
            # Parse the count parameter
            top_n = int(count)
            return get_recommendations(user_id, top_n)
        except ValueError:
            return f"Error: Invalid count '{count}'. Please provide a valid number."
        except Exception as e:
            return f"Error getting recommendations: {str(e)}"

    def enhanced_search_tool(query: str, top_k: str = "5") -> str:
        """Enhanced product search with better formatting"""
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

            # Use the global vectorstore instead of loading FAISS
            docs = vectorstore.similarity_search(search_query, k=top_k)
            
            if not docs:
                return f"No products found matching: {search_query}"
            
            result = f"Found {len(docs)} products related to '{search_query}':\n\n"
            
            for i, doc in enumerate(docs, 1):
                metadata = doc.metadata
                # Format exactly as requested
                result += f"{i}. **Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*\n\n"
            
            return result
        except ValueError:
            return f"Error: Invalid count '{top_k}'. Please provide a valid number."
        except Exception as e:
            return f"Error searching products: {str(e)}"
    
    def get_product_details(product_id: str) -> str:
        """Get detailed information about a specific product by ID"""
        try:
            product_id_int = int(product_id)
            
            # Create a search query for this specific product
            search_query = f"product_id:{product_id_int}"
            docs = vectorstore.similarity_search(search_query, k=1)
            
            if docs:
                metadata = docs[0].metadata
                # Format exactly as requested
                result = f"**Product**: *{metadata.get('product_name', 'Unknown Product')}*; **Aisle**: *{metadata.get('aisle', 'Unknown')}*; **Department**: *{metadata.get('department', 'Unknown')}*"
                return result
            else:
                return f"Product ID {product_id_int} not found in database."
                
        except ValueError:
            return f"Error: Product ID '{product_id}' is not a valid integer."
        except Exception as e:
            return f"Error getting product details: {str(e)}"

    def get_similar_users_tool(user_id: str, count: str = "5") -> str:
        """Get similar users to the given user_id, formatted for markdown output."""
        try:
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