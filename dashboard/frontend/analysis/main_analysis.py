import streamlit as st
import pandas as pd
import sys
import os

# Add the parent directory to the path for data_loader
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import modular components
from data_loader import load_data
from sidebar import render_sidebar
from tab1_overview import render_overview_tab
from tab2_product_affinity import render_product_affinity_tab
from tab3_customer_journey import render_customer_journey_tab
from tab4_lifetime_value import render_lifetime_value_tab
from tab5_churn_analysis import render_churn_analysis_tab

def main():
    """Main Streamlit dashboard application - Analysis Home Page"""
    
    st.title("🛒 Customer Behavior Analysis Dashboard")
    st.markdown("### Advanced Analytics with Optimized Performance")
    
    # Navigation info
    # st.info("💡 **Navigation**: Use the sidebar to switch between pages. The AI Chat Assistant is available as a separate page.")
    
    # Load data
    with st.spinner("Loading data..."):
        df, orders, products, departments, aisles = load_data()
    
    # st.success(f"✅ Data loaded successfully! {len(df):,} records processed.")
    
    # Render sidebar and get filters
    selected_departments = render_sidebar(departments)
    
    # Filter data based on selected departments
    filtered_df = df[df['department'].isin(selected_departments)]
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "🕸️ Product Affinity", 
        "🛤️ Customer Journey", 
        "💰 Lifetime Value", 
        "⚠️ Churn Analysis"
    ])
    
    # Render each tab
    with tab1:
        render_overview_tab(filtered_df, departments)
    
    with tab2:
        render_product_affinity_tab(filtered_df)
    
    with tab3:
        render_customer_journey_tab(filtered_df)
    
    with tab4:
        render_lifetime_value_tab(filtered_df)
    
    with tab5:
        render_churn_analysis_tab(filtered_df)
    
    # Footer
    st.markdown("---")
    st.markdown("📊 **Dashboard created with Streamlit & Plotly** | Data: E-commerce Customer Behavior")

if __name__ == "__main__":
    main() 