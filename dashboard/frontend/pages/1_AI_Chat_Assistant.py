import streamlit as st

# Set page config for the chat page - MUST be first!
st.set_page_config(
    page_title="AI Chat Assistant",
    layout="wide",
    page_icon="🤖"
)

import pandas as pd
import os
import sys

# Add the chat directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'chat'))

# Import chat interface
from chat_interface import render_chat_interface

def main():
    """AI Product Assistant - Chat Page"""
    
    st.title("🤖 AI Product Assistant")
    st.markdown("### Chat with our AI assistant to get personalized product recommendations!")
    
    # Add some spacing
    st.markdown("")
    
    # Render the chat interface with full width
    render_chat_interface()
    
    # Footer
    # st.markdown("---")
    # st.markdown("🤖 **AI Assistant powered by Streamlit** | Get personalized product recommendations")

# Call the main function
main() 