import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from analysis import create_churn_analysis

def render_churn_analysis_tab(filtered_df):
    """Render the customer churn analysis tab"""
    
    st.header("Customer Churn Analysis")
    st.markdown("Identify customers at risk of churning")
    
    churn_fig, churn_indicators = create_churn_analysis(filtered_df)
    st.plotly_chart(churn_fig, use_container_width=True)
    
    # Churn risk summary
    st.subheader("Churn Risk Summary")
    high_risk = churn_indicators[churn_indicators['max_days_between'] > 30]
    st.metric("High Churn Risk Customers", len(high_risk))
    
    # Show high-risk customers
    if len(high_risk) > 0:
        st.write("**High-Risk Customers (max days between orders > 30):**")
        st.dataframe(high_risk.head(10)) 