import streamlit as st
import sys
import os

# Add the analysis directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'analysis'))

# Import the analysis main function
from main_analysis import main

# Set page config for multi-page app
st.set_page_config(
    page_title="Analysis", 
    layout="wide",
    page_icon="📊"
)

# Run the analysis dashboard as home page
main() 