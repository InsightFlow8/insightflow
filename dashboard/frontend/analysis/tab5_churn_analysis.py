import streamlit as st
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

def render_churn_analysis_tab(athena_analyzer):
    """Render the customer churn analysis tab using Athena"""
    
    st.header("Customer Churn Analysis")
    st.markdown("Identify customers at risk of churning")
    
    # Add controls for the analysis
    col1, col2 = st.columns([2, 1])
    
    with col1:
        use_fast_analysis = st.checkbox("Use fast analysis (materialized view)", value=True, key="churn_analysis_fast")
    
    with col2:
        max_rows = st.number_input("Maximum rows to return", 1000, 50000, 10000, key="churn_analysis_max_rows")
    
    # Run the analysis
    with st.spinner("Running churn analysis..."):
        try:
            if use_fast_analysis:
                fig, df = athena_analyzer.create_churn_analysis_fast()
            else:
                fig, df = athena_analyzer.create_churn_analysis(use_cache=True)
            
            # Display the visualization
            st.plotly_chart(fig, use_container_width=True)
            
            # Show churn risk summary if available
            if len(df) > 0:
                st.subheader("Churn Risk Summary")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if 'max_days_between' in df.columns:
                        high_risk = len(df[df['max_days_between'] > 30])
                        st.metric("High Churn Risk (>30 days)", high_risk)
                    else:
                        st.metric("High Churn Risk", "N/A")
                
                with col2:
                    if 'avg_days_between' in df.columns:
                        avg_days = df['avg_days_between'].mean()
                        st.metric("Average Days Between Orders", f"{avg_days:.1f}")
                    else:
                        st.metric("Average Days Between Orders", "N/A")
                
                with col3:
                    if 'total_orders' in df.columns:
                        avg_orders = df['total_orders'].mean()
                        st.metric("Average Orders per Customer", f"{avg_orders:.1f}")
                    else:
                        st.metric("Average Orders per Customer", "N/A")
                
                # Show high-risk customers
                if 'max_days_between' in df.columns:
                    high_risk_df = df[df['max_days_between'] > 30]
                    if len(high_risk_df) > 0:
                        st.subheader("High-Risk Customers (max days between orders > 30)")
                        st.dataframe(high_risk_df.head(10))
                    else:
                        st.info("No high-risk customers found.")
                else:
                    st.info("Churn risk data not available.")
            else:
                st.warning("No churn analysis data found. Try adjusting the parameters.")
                
        except Exception as e:
            st.error(f"Error running churn analysis: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")
    
    # Show analysis info
    with st.expander("Analysis Information"):
        st.write("""
        **Customer Churn Analysis** identifies customers at risk of churning.
        
        - **High Risk**: Customers with >30 days between orders
        - **Average Days**: Typical time between customer orders
        - **Order Frequency**: How often customers place orders
        - **Data Source**: Raw order data from Athena
        """) 