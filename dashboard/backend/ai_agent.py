import logging
import uuid
from typing import Dict
from langchain_openai import ChatOpenAI
from langchain.agents import initialize_agent, AgentType
from langchain.memory import ConversationBufferMemory
from tools import create_enhanced_tools
from vector_store import get_vector_store

logger = logging.getLogger(__name__)

# Conversation sessions storage
conversation_sessions: Dict[str, ConversationBufferMemory] = {}

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
        
        # Get vector store
        vectorstore = get_vector_store()
        if vectorstore is None:
            logger.error("Vector store is None - cannot create tools")
            raise Exception("Vector store not initialized")
        
        logger.info("Vector store retrieved successfully")
        
        # Create tools dynamically with user_id context
        tools = create_enhanced_tools(vectorstore, user_id)
        logger.info(f"Created {len(tools)} tools")
        
        # Create the agent with format instructions
        agent = initialize_agent(
            tools,
            llm,
            agent=AgentType.CONVERSATIONAL_REACT_DESCRIPTION,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=5,
            early_stopping_method="generate",
            memory=memory,
            agent_kwargs={
                "system_message": """You are a product recommendation assistant. 
                
                IMPORTANT: When providing product information, always use consistent markdown formatting:
                
                **Product**: *[product_name]*  
                **Aisle**: *[aisle]*  
                **Department**: *[department]*
                
                For recommendations, include the reason and ensure diversity:
                **Product**: *[product_name]*  
                **Aisle**: *[aisle]*  
                **Department**: *[department]*  
                **Score**: *[score/explanation]*
                
                Always provide clear, formatted responses with unique and diverse product details using bold markdown headers.
                
                IMPORTANT: When using tools, always provide a final summary response that includes the tool results in a clear, formatted way."""
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
        query_lower = query.lower()
        
        # Handle greetings directly
        if any(word in query_lower for word in ["hello", "hi", "hey"]):
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
        if any(word in query_lower for word in ["recommend", "suggest", "buy", "should"]) and not user_id:
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
        
        # For general product queries (like "suggest some products"), let the agent handle it
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
        
        if any(word in query_lower for word in ["hello", "hi", "hey"]):
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