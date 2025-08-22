import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

def render_lifetime_value_tab(athena_analyzer, force_refresh: bool = False):
    """Render the customer lifetime value analysis tab using Athena"""
    
    st.header("Customer Lifetime Value Analysis")
    st.markdown("Segment customers by their value and behavior patterns")
    
    # Add controls for the analysis
    col1, col2 = st.columns([2, 1])
    
    with col1:
        use_fast_analysis = st.checkbox("Use fast analysis (materialized view)", value=True, key="lifetime_value_fast")
    
    with col2:
        max_rows = st.number_input("Maximum rows to return", 1000, 50000, 5000, key="lifetime_value_max_rows")
    
    # Run the analysis
    with st.spinner("Running lifetime value analysis..."):
        try:
            if use_fast_analysis:
                fig, df = athena_analyzer.create_lifetime_value_analysis_fast(force_refresh=force_refresh, max_rows=max_rows)
            else:
                fig, df = athena_analyzer.create_lifetime_value_analysis(use_cache=True, max_rows=max_rows)
            
            # Display the visualization
            st.plotly_chart(fig, use_container_width=True)
            
            # Show customer segments if available
            if len(df) > 0:
                st.subheader("Customer Segments")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if 'customer_segment' in df.columns:
                        high_value = len(df[df['customer_segment'] == 'High Value'])
                        st.metric("High-Value Customers", high_value)
                    else:
                        # Calculate based on total_orders
                        high_value = len(df[df['total_orders'] >= 10])
                        st.metric("High-Value Customers (10+ orders)", high_value)
                
                with col2:
                    if 'customer_segment' in df.columns:
                        medium_value = len(df[df['customer_segment'] == 'Medium Value'])
                        st.metric("Medium-Value Customers", medium_value)
                    else:
                        medium_value = len(df[(df['total_orders'] >= 5) & (df['total_orders'] < 10)])
                        st.metric("Medium-Value Customers (5-9 orders)", medium_value)
                
                with col3:
                    if 'customer_segment' in df.columns:
                        low_value = len(df[df['customer_segment'] == 'Low Value'])
                        st.metric("Low-Value Customers", low_value)
                    else:
                        low_value = len(df[df['total_orders'] < 5])
                        st.metric("Low-Value Customers (<5 orders)", low_value)
                
                # Show additional metrics
                st.subheader("📊 Customer Metrics")
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'total_orders' in df.columns:
                        avg_orders = df['total_orders'].mean()
                        st.info(f"**Average Orders per Customer:** {avg_orders:.1f}")
                    
                    if 'total_items' in df.columns:
                        avg_items = df['total_items'].mean()
                        st.info(f"**Average Items per Customer:** {avg_items:.1f}")
                
                with col2:
                    if 'total_reorders' in df.columns and 'total_items' in df.columns:
                        reorder_rate = (df['total_reorders'] / df['total_items']).mean() * 100
                        st.info(f"**Average Reorder Rate:** {reorder_rate:.1f}%")
                    
                    if 'avg_order_size' in df.columns:
                        avg_order_size = df['avg_order_size'].mean()
                        st.info(f"**Average Order Size:** {avg_order_size:.1f} items")
            else:
                st.warning("No lifetime value data found. Try adjusting the parameters.")
                
        except Exception as e:
            st.error(f"Error running lifetime value analysis: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")
    
    # Show analysis info
    with st.expander("Analysis Information"):
        st.write("""
        **Customer Lifetime Value Analysis** segments customers by their value and behavior.
        
        - **High Value**: Customers with 10+ orders
        - **Medium Value**: Customers with 5-9 orders
        - **Low Value**: Customers with <5 orders
        - **Data Source**: Raw order data from Athena
        """) 