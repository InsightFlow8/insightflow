import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

def render_customer_journey_tab(athena_analyzer, force_refresh: bool = False):
    """Render the customer journey and behavior analysis tab using Athena"""
    
    st.header("Customer Journey & Behavior Analysis")
    st.markdown("Comprehensive view of customer types, order patterns, and purchasing behavior")
    
    # Add controls for the analysis
    col1, col2 = st.columns([2, 1])
    
    with col1:
        use_fast_analysis = st.checkbox("Use fast analysis (materialized view)", value=True, key="customer_journey_fast")
    
    with col2:
        max_rows = st.number_input("Maximum rows to return", 1000, 50000, 5000, key="customer_journey_max_rows")
    
    # Run the analysis
    with st.spinner("Running customer journey analysis..."):
        try:
            if use_fast_analysis:
                fig, df = athena_analyzer.create_customer_journey_analysis_fast(force_refresh=force_refresh, max_rows=max_rows)
            else:
                fig, df = athena_analyzer.create_customer_journey_analysis(use_cache=True, max_rows=max_rows)
            
            # Display the visualization
            st.plotly_chart(fig, use_container_width=True)
            
            # Show key metrics if available
            if len(df) > 0:
                # Enhanced journey insights
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if 'total_orders' in df.columns:
                        st.metric("Total Orders", f"{df['total_orders'].iloc[0]:,}")
                    else:
                        st.metric("Total Orders", "N/A")
                
                with col2:
                    if 'total_customers' in df.columns:
                        st.metric("Total Customers", f"{df['total_customers'].iloc[0]:,}")
                    else:
                        st.metric("Total Customers", "N/A")
                
                with col3:
                    if 'first_time_customers' in df.columns and 'repeat_customers' in df.columns:
                        try:
                            repeat_rate = df['repeat_customers'].iloc[0] / (df['first_time_customers'].iloc[0] + df['repeat_customers'].iloc[0]) * 100
                            st.metric("Repeat Customer Rate", f"{repeat_rate:.1f}%")
                        except (KeyError, IndexError, ZeroDivisionError):
                            st.metric("Repeat Customer Rate", "N/A")
                    else:
                        st.metric("Repeat Customer Rate", "N/A")
                
                with col4:
                    if 'total_items' in df.columns:
                        st.metric("Total Items", f"{df['total_items'].iloc[0]:,}")
                    else:
                        st.metric("Total Items", "N/A")
                
                # Additional insights
                st.subheader("📊 Key Insights")
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'reordered_items' in df.columns and 'total_items' in df.columns:
                        try:
                            reorder_rate = df['reordered_items'].iloc[0] / df['total_items'].iloc[0] * 100
                            st.info(f"**Reordered Items:** {df['reordered_items'].iloc[0]:,} ({reorder_rate:.1f}%)")
                        except (KeyError, IndexError, ZeroDivisionError):
                            st.info("**Reordered Items:** N/A")
                    else:
                        st.info("**Reordered Items:** N/A")
                    
                    if 'small_orders' in df.columns and 'medium_orders' in df.columns and 'large_orders' in df.columns:
                        try:
                            total_orders = df['small_orders'].iloc[0] + df['medium_orders'].iloc[0] + df['large_orders'].iloc[0]
                            avg_order_size = (df['small_orders'].iloc[0] * 3 + df['medium_orders'].iloc[0] * 10.5 + df['large_orders'].iloc[0] * 20) / total_orders
                            st.info(f"**Average Order Size:** {avg_order_size:.1f} items")
                        except (KeyError, IndexError, ZeroDivisionError):
                            st.info("**Average Order Size:** N/A")
                    else:
                        st.info("**Average Order Size:** N/A")
                
                with col2:
                    if 'first_time_customers' in df.columns and 'repeat_customers' in df.columns:
                        try:
                            total_customers = df['first_time_customers'].iloc[0] + df['repeat_customers'].iloc[0]
                            loyal_customers = df['repeat_customers'].iloc[0]
                            st.info(f"**Loyal Customers (2+ orders):** {loyal_customers:,} ({loyal_customers/total_customers*100:.1f}%)")
                        except (KeyError, IndexError, ZeroDivisionError):
                            st.info("**Loyal Customers (2+ orders):** N/A")
                    else:
                        st.info("**Loyal Customers (2+ orders):** N/A")
                    
                    if 'total_orders' in df.columns and 'total_customers' in df.columns:
                        try:
                            avg_orders_per_customer = df['total_orders'].iloc[0] / df['total_customers'].iloc[0]
                            st.info(f"**Avg Orders per Customer:** {avg_orders_per_customer:.1f}")
                        except (KeyError, IndexError, ZeroDivisionError):
                            st.info("**Avg Orders per Customer:** N/A")
                    else:
                        st.info("**Avg Orders per Customer:** N/A")
                
                # Show data columns info for debugging
                with st.expander("🔍 Data Structure"):
                    st.write(f"**DataFrame Shape:** {df.shape}")
                    st.write(f"**Available Columns:** {list(df.columns)}")
                    st.write("**Sample Data:**")
                    st.dataframe(df.head())
            else:
                st.warning("No customer journey data found. Try adjusting the parameters.")
                
        except Exception as e:
            st.error(f"Error running customer journey analysis: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")
            
            # Show debugging information
            with st.expander("🔍 Debug Information"):
                st.write(f"**Error Type:** {type(e).__name__}")
                st.write(f"**Error Message:** {str(e)}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    st.write("**Traceback:**")
                    st.code(traceback.format_exc())
    
    # Show analysis info
    with st.expander("Analysis Information"):
        st.write("""
        **Customer Journey Analysis** provides insights into customer behavior patterns.
        
        - **Customer Types**: First-time vs repeat customers
        - **Order Patterns**: Order size distribution and frequency
        - **Reorder Behavior**: Analysis of customer loyalty
        - **Data Source**: Raw order data from Athena
        """) 