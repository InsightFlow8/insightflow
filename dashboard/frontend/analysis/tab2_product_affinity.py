import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

from analysis import create_product_affinity_simple

def render_product_affinity_tab(filtered_df):
    """Render the product affinity analysis tab"""
    
    st.header("Product Affinity Analysis")
    st.markdown("Discover which products are frequently purchased together")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Product affinity analysis
        pair_counts, product_names, product_counts = create_product_affinity_simple(filtered_df)
        
        # Show top product pairs
        st.subheader("Top Product Pairs")
        top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        
        for (prod1, prod2), count in top_pairs:
            prod1_name = product_names.get(prod1, f'Product {prod1}')
            prod2_name = product_names.get(prod2, f'Product {prod2}')
            st.write(f"**{prod1_name[:40]}...** + **{prod2_name[:40]}...** ({count} times)")
    
    with col2:
        st.subheader("Top Products by Frequency")
        top_products = product_counts.head(10)
        for prod_id, count in top_products.items():
            prod_name = product_names.get(prod_id, f'Product {prod_id}')
            st.write(f"**{prod_name[:30]}...** ({count} orders)") 