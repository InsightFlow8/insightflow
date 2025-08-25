import streamlit as st
import plotly.express as px
import pandas as pd
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
            df_summary = athena_analyzer.get_data_summary(use_cache=False, departments=selected_departments)

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


                # 数据
                st.subheader("Dataset Overview by Metric")

                # 1) 取 Athena 汇总数据（保持你们原有的 analyzer 调用）
                df_summary = athena_analyzer.get_data_summary(
                    use_cache=False, 
                    departments=selected_departments
                )

                # 2) 规范列名 & 增加占比（可选）
                # 如果 df_summary 已经是 columns = ["metric", "value"]，这段能直接用；
                # 若不是，按注释把列名对齐即可。
                if "metric" in df_summary.columns and "value" in df_summary.columns:
                    table_df = (
                        df_summary.rename(columns={"metric": "Metric", "value": "Count"})
                                .assign(Share=lambda d: (d["Count"] / d["Count"].sum()).round(4))
                                .sort_values("Count", ascending=False)
                    )
                else:
                    # 如果 get_data_summary 返回的是一个 dict，比如 {"total_items": 123, ...}
                    # 用下面这段把它转成表格
                    m = df_summary if isinstance(df_summary, dict) else df_summary.to_dict()
                    table_df = pd.DataFrame([
                        {"Metric": "Total Items",  "Count": m.get("total_items", 0)},
                        {"Metric": "Total Orders", "Count": m.get("total_orders", 0)},
                        {"Metric": "Total Users",  "Count": m.get("total_users", 0)},
                        {"Metric": "Total Products","Count": m.get("total_products", 0)},
                    ])
                    table_df["Share"] = (table_df["Count"] / table_df["Count"].sum()).round(4)

                # 3) 表格展示（交互）
                st.dataframe(
                    table_df,
                    use_container_width=True,
                    hide_index=True
                )


            else:
                st.warning("No data summary available. Please check your AWS credentials and S3 bucket access.")

        except Exception as e:
            st.error(f"Error loading data summary: {e}")
            st.info("Please check your AWS credentials and S3 bucket access.")

    # Show data source information
    with st.expander("Data Source Information"):
        st.write("""
        **Data Source**: Amazon Athena with S3 storage

        - **Database**: insightflow_imba_clean_data_catalog
        - **Tables**: after_clean_orders, after_clean_products_prior, after_clean_order_products_train, after_clean_products, after_clean_departments, after_clean_aisles
        - **Storage**: S3 bucket with Parquet format
        - **Analysis**: Real-time queries with caching support
        """)
