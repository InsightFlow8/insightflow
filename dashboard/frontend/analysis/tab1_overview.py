import streamlit as st
import plotly.express as px
import sys
import os

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))

def render_overview_tab(athena_analyzer, selected_departments=None):
    """Render the overview tab with data summary using Athena"""
    
    st.header("Data Overview")
    st.markdown("Summary statistics and key metrics from the e-commerce dataset")
    
    # Show selected departments if any are selected
    if selected_departments and "All Departments" not in selected_departments:
        st.info(f"📊 **Filtered by departments:** {', '.join(selected_departments)}")
    elif selected_departments and "All Departments" in selected_departments:
        st.info("📊 **Showing all departments**")
    
    # Get data summary from Athena
    with st.spinner("Loading data summary..."):
        try:
            df_summary = athena_analyzer.get_data_summary(use_cache=True, departments=selected_departments)
            
            if len(df_summary) > 0:
                # Display key metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_users = df_summary[df_summary['metric'] == 'Total Users']['value'].iloc[0]
                    st.metric("Total Users", f"{total_users:,}")
                
                with col2:
                    total_orders = df_summary[df_summary['metric'] == 'Total Orders']['value'].iloc[0]
                    st.metric("Total Orders", f"{total_orders:,}")
                
                with col3:
                    total_products = df_summary[df_summary['metric'] == 'Total Products']['value'].iloc[0]
                    st.metric("Total Products", f"{total_products:,}")
                
                with col4:
                    total_items = df_summary[df_summary['metric'] == 'Total Items']['value'].iloc[0]
                    st.metric("Total Items", f"{total_items:,}")
                
                # Show additional metrics
                st.subheader("📊 Additional Metrics")
                col1, col2 = st.columns(2)
                
                with col1:
                    total_departments = df_summary[df_summary['metric'] == 'Total Departments']['value'].iloc[0]
                    st.info(f"**Total Departments:** {total_departments}")
                    
                    total_aisles = df_summary[df_summary['metric'] == 'Total Aisles']['value'].iloc[0]
                    st.info(f"**Total Aisles:** {total_aisles}")
                
                with col2:
                    avg_items_per_order = total_items / total_orders if total_orders > 0 else 0
                    st.info(f"**Average Items per Order:** {avg_items_per_order:.1f}")
                    
                    avg_orders_per_user = total_orders / total_users if total_users > 0 else 0
                    st.info(f"**Average Orders per User:** {avg_orders_per_user:.1f}")
                
                # Create a summary chart
                st.subheader("📈 Data Summary Chart")
                summary_data = df_summary.copy()
                summary_data = summary_data[summary_data['metric'].isin(['Total Users', 'Total Orders', 'Total Products', 'Total Items'])]
                
                fig = px.bar(
                    summary_data, 
                    x='metric', 
                    y='value',
                    title="Dataset Overview",
                    labels={'metric': 'Metric', 'value': 'Count'}
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                
            else:
                st.warning("No data summary available. Please check your AWS credentials and S3 bucket access.")
                
        except Exception as e:
            st.error(f"Error loading data summary: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")
    
    # Show data source information
    with st.expander("Data Source Information"):
        st.write("""
        **Data Source**: Amazon Athena with S3 storage
        
        - **Database**: insightflow_imba_raw_data_catalog
        - **Tables**: raw_orders, raw_order_products_prior, raw_order_products_train, raw_products, raw_departments, raw_aisles
        - **Storage**: S3 bucket with Parquet format
        - **Analysis**: Real-time queries with caching support
        """) 