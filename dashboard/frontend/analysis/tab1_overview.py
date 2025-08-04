import streamlit as st
import plotly.express as px

def render_overview_tab(filtered_df, departments):
    """Render the overview tab with data summary and department distribution"""
    
    st.header("Data Overview")
    st.write(f"**Total Records:** {len(filtered_df):,}")
    st.write(f"**Unique Orders:** {filtered_df['order_id'].nunique():,}")
    st.write(f"**Unique Customers:** {filtered_df['user_id'].nunique():,}")
    st.write(f"**Unique Products:** {filtered_df['product_id'].nunique():,}")
    
    # Department distribution
    dept_counts = filtered_df['department'].value_counts()
    fig = px.bar(x=dept_counts.index, y=dept_counts.values, title="Orders by Department")
    st.plotly_chart(fig, use_container_width=True) 