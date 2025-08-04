import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from analysis import create_lifetime_value_analysis

def render_lifetime_value_tab(filtered_df):
    """Render the customer lifetime value analysis tab"""
    
    st.header("Customer Lifetime Value Analysis")
    st.markdown("Segment customers by their value and behavior patterns")
    
    ltv_fig, customer_metrics = create_lifetime_value_analysis(filtered_df)
    st.plotly_chart(ltv_fig, use_container_width=True)
    
    # Customer segments
    st.subheader("Customer Segments")
    col1, col2 = st.columns(2)
    
    with col1:
        high_value = customer_metrics[customer_metrics['total_orders'] > customer_metrics['total_orders'].quantile(0.8)]
        st.metric("High-Value Customers", len(high_value))
    
    with col2:
        loyal_customers = customer_metrics[customer_metrics['reorder_rate'] > customer_metrics['reorder_rate'].quantile(0.8)]
        st.metric("Loyal Customers", len(loyal_customers)) 