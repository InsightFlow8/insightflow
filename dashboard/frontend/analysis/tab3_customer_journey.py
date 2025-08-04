import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from analysis import create_customer_journey_flow

def render_customer_journey_tab(filtered_df):
    """Render the customer journey and behavior analysis tab"""
    
    st.header("Customer Journey & Behavior Analysis")
    st.markdown("Comprehensive view of customer types, order patterns, and purchasing behavior")
    
    journey_fig = create_customer_journey_flow(filtered_df)
    st.plotly_chart(journey_fig, use_container_width=True)
    
    # Enhanced journey insights
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_items = filtered_df.groupby('order_id')['add_to_cart_order'].max().mean()
        st.metric("Average Items per Order", f"{avg_items:.1f}")
    
    with col2:
        reorder_rate = filtered_df['reordered'].mean() * 100
        st.metric("Overall Reorder Rate", f"{reorder_rate:.1f}%")
    
    with col3:
        customer_order_counts = filtered_df.groupby('user_id')['order_id'].nunique()
        repeat_customer_rate = (customer_order_counts > 1).mean() * 100
        st.metric("Repeat Customer Rate", f"{repeat_customer_rate:.1f}%")
    
    with col4:
        total_customers = filtered_df['user_id'].nunique()
        st.metric("Total Customers", f"{total_customers:,}")
    
    # Additional insights
    st.subheader("📊 Key Insights")
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer loyalty analysis
        customer_order_counts = filtered_df.groupby('user_id')['order_id'].nunique()
        loyal_customers = (customer_order_counts >= 5).sum()
        st.info(f"**Loyal Customers (5+ orders):** {loyal_customers:,} ({loyal_customers/total_customers*100:.1f}%)")
        
        # Order size analysis
        order_sizes = filtered_df.groupby('order_id')['add_to_cart_order'].max()
        avg_order_size = order_sizes.mean()
        st.info(f"**Average Order Size:** {avg_order_size:.1f} items")
    
    with col2:
        # Reorder behavior
        total_items = len(filtered_df)
        reordered_items = filtered_df['reordered'].sum()
        st.info(f"**Reordered Items:** {reordered_items:,} ({reordered_items/total_items*100:.1f}%)")
        
        # Customer engagement
        avg_orders_per_customer = filtered_df.groupby('user_id')['order_id'].nunique().mean()
        st.info(f"**Avg Orders per Customer:** {avg_orders_per_customer:.1f}") 