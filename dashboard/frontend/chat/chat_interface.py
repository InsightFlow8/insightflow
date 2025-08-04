import streamlit as st
import requests
import json
import time
import os

def render_chat_interface():
    """Render the chat interface with AI-powered product recommendations"""
    
    # Add CSS for scrollable chat container
    st.markdown("""
    <style>
    .stChatInput {
        position: sticky;
        bottom: 0;
        background: white;
        z-index: 1000;
    }
    .settings-container {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Get backend URL from environment or use default
    backend_url = os.getenv("BACKEND_URL", "http://backend:8000")
    
    # Initialize session state for chat
    if 'session_id' not in st.session_state:
        st.session_state.session_id = None
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None
    
    # Session state for chat history
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    def process_chat_query(query):
        """Process a chat query and add to chat history"""
        # Add user message to chat history
        st.session_state.chat_history.append(("user", query))
        
        # Process the request
        try:
            # Prepare request data
            request_data = {
                "query": query,
                "user_id": st.session_state.user_id,
                "session_id": st.session_state.session_id
            }
            
            # Send request to backend with spinner in the placeholder location
            with spinner_placeholder:
                with st.spinner("🤖 AI is thinking..."):
                    response = requests.post(
                        f"{backend_url}/chat",
                        json=request_data,
                        timeout=120
                    )
            
            if response.status_code == 200:
                result = response.json()
                
                # Check if response has error
                if "error" in result:
                    assistant_response = f"Error: {result['error']}"
                else:
                    assistant_response = result.get("answer", "I'm sorry, I couldn't process your request.")
                
                # Update session ID if provided
                if "session_id" in result:
                    st.session_state.session_id = result["session_id"]
                
                # Add assistant response to chat history
                st.session_state.chat_history.append(("ai", assistant_response))
                
            else:
                error_msg = f"Error: Backend returned status {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg += f" - {error_data.get('detail', 'Unknown error')}"
                except:
                    pass
                
                st.session_state.chat_history.append(("ai", error_msg))
                
        except requests.exceptions.RequestException as e:
            error_msg = f"Error connecting to backend: {str(e)}"
            st.session_state.chat_history.append(("ai", error_msg))
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            st.session_state.chat_history.append(("ai", error_msg))
    
    # Sidebar for settings and quick actions
    with st.sidebar:
        st.subheader("⚙️ Chat Settings")
        
        # User ID input
        user_id_input = st.text_input(
            "User ID (for personalized recommendations)",
            value=st.session_state.user_id or "",
            help="Enter your user ID to get personalized product recommendations"
        )
        
        if user_id_input != st.session_state.user_id:
            st.session_state.user_id = user_id_input
            st.session_state.chat_history = []  # Clear chat when user changes
            st.session_state.session_id = None
        
        # Clear chat button
        if st.button("🗑️ Clear Chat"):
            st.session_state.chat_history = []
            st.session_state.session_id = None
            st.rerun()
    
    # Main chat interface - full width
    # Chat messages display area with fixed height and scroll
    st.subheader("💬 Chat History")
    
    # Display chat history directly
    for role, msg in st.session_state.chat_history:
        if role.lower() == "user":
            with st.chat_message("user"):
                st.markdown(msg)
        elif role.lower() == "ai":
            with st.chat_message("assistant"):
                st.markdown(msg)
    
    # Fixed chat input at the bottom
    # st.markdown("---")  # Separator line
    
    # Create spinner placeholder right above text input
    spinner_placeholder = st.empty()
    
    user_query = st.chat_input("Ask me about products, recommendations, or anything else!")
    
    # Process user input if provided
    if user_query:
        process_chat_query(user_query)
        st.rerun()
    
    # Quick actions below text input
    st.markdown("")
    # st.subheader("💡 Quick Actions")
    st.markdown("**Try these examples:**")
    
    # Example queries in a horizontal layout
    example_queries = [
        "What should I buy?",
        "suggest products",
        "Tell me about organic fruits",
        "Find dairy products",
        "What are some healthy snacks?",
        "Show me 6 beverages"
    ]
    
    cols = st.columns(len(example_queries))
    for i, query in enumerate(example_queries):
        with cols[i]:
            if st.button(query, key=f"example_{query}"):
                process_chat_query(query)
                st.rerun() 