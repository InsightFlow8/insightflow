import logging
import uuid
from typing import Dict
from langchain_openai import ChatOpenAI
from langchain.agents import initialize_agent, AgentType
from langchain.memory import ConversationBufferMemory
from tools import create_enhanced_tools
from vector_store_s3 import get_s3_vector_store

logger = logging.getLogger(__name__)

# Conversation sessions storage
conversation_sessions: Dict[str, ConversationBufferMemory] = {}

def is_greeting(text):
    """Check if the text is a greeting by looking for exact word matches"""
    text_lower = text.lower().strip()
    words = text_lower.split()
    
    # Define exact greeting phrases and words
    exact_greetings = [
        "hello", "hi", "hey", 
        "good morning", "good afternoon", "good evening",
        "how are you"
    ]
    
    # Check for exact phrase matches first
    for greeting in exact_greetings:
        if greeting == text_lower:
            return True
    
    # Check for single greeting words (only if the text is very short)
    if len(words) <= 2:
        single_greetings = ["hello", "hi", "hey"]
        for word in words:
            if word in single_greetings:
                return True
    
    return False

def create_hybrid_agent(session_id: str = None, user_id: str = None):
    """Create a hybrid agent with conversation memory"""
    
    try:
        logger.info(f"Creating hybrid agent - Session ID: {session_id}, User ID: {user_id}")
        
        # Initialize LLM with system message
        llm = ChatOpenAI(model="gpt-4o", temperature=0)
        logger.info("LLM initialized successfully")
        
        # Get or create conversation memory for this session
        if session_id and session_id in conversation_sessions:
            memory = conversation_sessions[session_id]
            logger.info(f"Using existing memory for session: {session_id}")
        else:
            memory = ConversationBufferMemory(
                memory_key="chat_history",
                return_messages=True
            )
            if session_id:
                conversation_sessions[session_id] = memory
            logger.info(f"Created new memory for session: {session_id}")
        
        # Get S3 Vectors store
        vectorstore = get_s3_vector_store()
        if vectorstore is None:
            logger.error("S3 Vectors store is None - cannot create tools")
            raise Exception("S3 Vectors store not initialized")
        
        logger.info("S3 Vectors store retrieved successfully")
        
        # Create tools dynamically with user_id context
        tools = create_enhanced_tools(vectorstore, user_id)
        # Add structured recommendation tool for robust output parsing
        from tools import recommendation_tool_structured
        tools.append(recommendation_tool_structured)
        logger.info(f"Created {len(tools)} tools (including structured output tool)")
        
        # Create the agent with format instructions
        agent = initialize_agent(
            tools,
            llm,
            agent=AgentType.OPENAI_FUNCTIONS,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=5,
            early_stopping_method="generate",
            memory=memory,
            agent_kwargs={
                "system_message": """You are a product recommendation assistant with access to a real product database. 

CRITICAL INSTRUCTIONS:
1. ALWAYS use tools to explore the product database when users ask about products, categories, or recommendations
2. Use search_product_database tool for general product queries like "organic fruits", "dairy products", "healthy snacks"
3. Use get_product_recommendations tool when users ask for personalized recommendations
4. Use get_product_details tool when users ask about specific product IDs
5. Use get_similar_users tool when users ask about similar customers
6. Use get_product_id_by_name tool when users ask about specific product names or when you need to find a product ID
7. Use get_user_product_probability tool when users ask about purchase probability or likelihood to buy specific products (for product name queries, always pass the name, not just the ID, to get multiple related products and their scores; never output 'undefined', 'null', or fallback text. If you cannot parse the output, return the tool output as-is.)

FORMATTING REQUIREMENTS:
- Always format product information as: **Product**: *[product_name]*; **Aisle**: *[aisle]*; **Department**: *[department]*
- For recommendations, include: **Product**: *[product_name]*; **Aisle**: *[aisle]*; **Department**: *[department]*; **Normalized Score**: *[score]*

EXAMPLES OF WHEN TO USE TOOLS:
- "Tell me about organic fruits" → Use search_product_database with "organic fruits"
- "What should I buy?" → Use get_product_recommendations
- "Find dairy products" → Use search_product_database with "dairy"
- "Tell me about product 3" → Use get_product_details with "3"
- "What's the product ID for Organic Avocado?" → Use get_product_id_by_name with "Organic Avocado"
- "Will user 123 buy Organic Avocado?" → First use get_product_id_by_name with "Organic Avocado", then use get_user_product_probability with "123, [product_id]"

WORKFLOW FOR PRODUCT PROBABILITY QUERIES:
1. If user asks about purchase probability for a product name (not ID), first use get_product_id_by_name to find the product ID
2. Then use get_user_product_probability with the user ID and product ID

CRITICAL OUTPUT REQUIREMENTS:
- When using tools, return ONLY the raw tool output exactly as provided
- Do NOT add any conversational text, explanations, or commentary
- Do NOT wrap tool output in quotes or add extra formatting
- Do NOT say things like "I'm sorry for the confusion" or "Let me provide you with..."
- The tool output should be the complete and final response

NEVER provide general information about products without using tools to search the actual database."""
            }
        )
        
        logger.info("Agent created successfully")
        return agent
        
    except Exception as e:
        logger.error(f"Error creating hybrid agent: {e}")
        raise

def clear_conversation_history(session_id: str):
    """Clear conversation history for a session"""
    if session_id in conversation_sessions:
        # Clear the memory for this session
        conversation_sessions[session_id].clear()
        logger.info(f"Cleared conversation history for session: {session_id}")
        return True
    else:
        logger.warning(f"Session {session_id} not found for clearing history")
        return False

def get_session_id():
    """Generate a new session ID"""
    return str(uuid.uuid4())

def process_chat_query(query: str, session_id: str = None, user_id: str = None):
    """Process a chat query using the hybrid agent"""
    
    # Generate session ID if not provided
    if not session_id:
        session_id = get_session_id()

    try:
        logger.info(f"Processing query: '{query}' with session_id: {session_id}, user_id: {user_id}")
        
        # Pre-check for greetings and recommendation requests to provide direct responses
        query_lower = query.lower().strip()
        
        if is_greeting(query_lower):
            logger.info("Handling greeting query")
            if user_id:
                response = f"Hello! I'm your product assistant. I can help you with product information and personalized recommendations. Your user ID is {user_id}, so I can provide personalized recommendations for you. Ask me 'What should I buy?' for personalized recommendations or ask about specific products and categories."
            else:
                response = "Hello! I'm your product assistant. I can help you with product information and personalized recommendations. Please enter your user ID above for personalized recommendations, or ask me about specific products and categories."
            
            return {
                "answer": response,
                "type": "direct_response",
                "session_id": session_id
            }
        
        # Handle recommendation requests without user ID directly
        if any(word in query_lower for word in ["recommend", "suggest", "buy", "should"]) and not user_id and not any(word in query_lower for word in ["show", "find", "search", "chips", "bread", "fruits", "vegetables", "dairy"]):
            logger.info("Handling recommendation request without user ID")
            response = "I'd be happy to provide personalized recommendations! Please enter your user ID in the field above and try again. For example, enter '123' in the User ID field, then ask 'What should I buy?'"
            
            return {
                "answer": response,
                "type": "direct_response",
                "session_id": session_id
            }
        
        # Handle recommendation requests with user ID - let the agent use the recommendation tool
        if any(word in query_lower for word in ["recommend", "suggest", "buy", "should"]) and user_id:
            logger.info(f"Handling recommendation request with user ID: {user_id} - using agent with tools")
            # Let the agent handle this with the recommendation tool
            agent = create_hybrid_agent(session_id, user_id)
            response = agent.run(query)
            logger.info(f"Agent response received: {response[:100]}...")
            
            return {
                "answer": response,
                "type": "hybrid_agent_response",
                "session_id": session_id
            }
        
        # For all other queries (including product searches like "show some chips"), let the agent handle it
        logger.info("Creating hybrid agent for query processing")
        # Create hybrid agent with session memory and user ID context
        agent = create_hybrid_agent(session_id, user_id)
        
        # Run the agent with timeout
        logger.info("Running agent with query")
        response = agent.run(query)
        logger.info(f"Agent response received: {response[:100]}...")
        
        return {
            "answer": response,
            "type": "hybrid_agent_response",
            "session_id": session_id
        }
        
    except Exception as e:
        logger.error(f"Agent error: {e}")
        
        # Provide a helpful fallback response based on the query
        query_lower = query.lower()
        
        if is_greeting(query_lower):
            if user_id:
                fallback_response = f"Hello! I'm your product assistant. I can help you with product information and personalized recommendations. Your user ID is {user_id}, so I can provide personalized recommendations for you. Ask me 'What should I buy?' for personalized recommendations or ask about specific products and categories."
            else:
                fallback_response = "Hello! I'm your product assistant. I can help you with product information and personalized recommendations. Please enter your user ID above for personalized recommendations, or ask me about specific products and categories."
        elif any(word in query_lower for word in ["recommend", "suggest", "buy", "should"]):
            if not user_id:
                # User is asking for recommendations without user ID
                fallback_response = "I'd be happy to provide personalized recommendations! Please enter your user ID in the field above and try again. For example, enter '123' in the User ID field, then ask 'What should I buy?'"
            else:
                fallback_response = "I'm here to help with product recommendations and information! You can ask me about specific products, categories, or request personalized recommendations. For example, try asking 'Tell me about organic fruits' or 'What should I buy?'"
        elif any(word in query_lower for word in ["product", "products", "find", "search"]):
            fallback_response = "I can help you find products! Try asking about specific categories like 'Tell me about soft drinks' or 'What fruits do you have?'"
        else:
            if user_id:
                fallback_response = f"I'm here to help you with product information and recommendations! Your user ID is {user_id}, so I can provide personalized recommendations. Ask me 'What should I buy?' for personalized recommendations or ask about specific products and categories."
            else:
                fallback_response = "I'm here to help you with product information and recommendations! Please enter your user ID above for personalized recommendations, or ask me about specific products and categories."
        
        return {
            "answer": fallback_response,
            "type": "fallback_response",
            "error": str(e),
            "session_id": session_id
        } 