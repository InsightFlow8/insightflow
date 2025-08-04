import streamlit as st

def render_sidebar(departments):
    """Render the sidebar with filters"""
    
    st.sidebar.header("📊 Dashboard Filters")
    
    # Department filter
    selected_departments = st.sidebar.multiselect(
        "Select Departments",
        options=departments['department'].unique(),
        default=departments['department'].unique()[:5]
    )
    
    return selected_departments 