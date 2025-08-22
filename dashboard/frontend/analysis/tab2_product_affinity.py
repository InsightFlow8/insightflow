import streamlit as st
import sys
import os
import pandas as pd

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

def render_product_affinity_tab(athena_analyzer, selected_departments=None, force_refresh: bool = False):
    """Render the product affinity analysis tab using Athena"""
    
    st.header("Product Affinity Analysis")
    st.markdown("Discover which products are frequently purchased together")
    
    # Show selected departments if any
    if selected_departments and "All Departments" not in selected_departments:
        st.info(f"📊 **Filtering by departments:** {', '.join(selected_departments)}")
    
    # Add controls for the analysis
    col1, col2 = st.columns([2, 1])
    
    with col1:
        top_products = st.slider("Number of top products to analyze", 10, 50, 20)
        use_fast_analysis = st.checkbox("Use fast analysis (materialized view)", value=True, key="product_affinity_fast")
    
    with col2:
        max_rows = st.number_input("Maximum rows to return", 1000, 50000, 5000, key="product_affinity_max_rows")
    
    # Run the analysis
    with st.spinner("Running product affinity analysis..."):
        try:
            if use_fast_analysis:
                fig, df = athena_analyzer.create_product_affinity_analysis_fast(
                    top_products=top_products, 
                    max_rows=max_rows,
                    departments=selected_departments,
                    force_refresh=force_refresh
                )
            else:
                fig, df = athena_analyzer.create_product_affinity_analysis(
                    top_products=top_products, 
                    use_cache=True, 
                    max_rows=max_rows,
                    departments=selected_departments
                )
            
            # Display the visualization
            st.plotly_chart(fig, use_container_width=True)
            
            # Show top product pairs
            if len(df) > 0:
                st.subheader("Top Product Pairs")
                top_pairs = df.head(15)
                
                # Show summary statistics
                if 'product1_department' in df.columns and 'product2_department' in df.columns:
                    st.write(f"**Total pairs found:** {len(df)}")
                    # Count unique departments more accurately
                    all_departments = pd.concat([df['product1_department'], df['product2_department']]).unique()
                    st.write(f"**Departments represented:** {len(all_departments)} unique departments")
                
                for _, row in top_pairs.iterrows():
                    # Show department information if available
                    dept_info = ""
                    if 'product1_department' in df.columns and 'product2_department' in df.columns:
                        dept_info = f" ({row['product1_department']} + {row['product2_department']})"
                    
                    st.write(f"**{row['product1_name'][:40]}...** + **{row['product2_name'][:40]}...** ({row['pair_count']} times){dept_info}")
            else:
                st.warning("No product pairs found. Try adjusting the parameters or selecting different departments.")
                
        except Exception as e:
            st.error(f"Error running product affinity analysis: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")
            
            # Show debugging information
            with st.expander("🔍 Debug Information"):
                st.write(f"**Selected Departments:** {selected_departments}")
                st.write(f"**Error Type:** {type(e).__name__}")
                st.write(f"**Error Message:** {str(e)}")
    
    # Show analysis info
    with st.expander("Analysis Information"):
        st.write("""
        **Product Affinity Analysis** identifies which products are frequently purchased together.
        
        - **Top Products**: Analyzes the most frequently purchased products
        - **Pair Count**: Number of times two products were purchased together
        - **Department Filtering**: Results can be filtered by specific departments
        - **Fast Analysis**: Uses materialized views for faster results
        - **Data Source**: Raw order data from Athena
        """) 