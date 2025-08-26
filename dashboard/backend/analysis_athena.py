import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import logging
import hashlib
import json
import os
import io
import numpy as np
from typing import Dict, Any, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AthenaAnalyzer:
    def __init__(self, region='ap-southeast-2', database='insightflow_imba_clean_data_catalog'):
        # Set AWS credentials from environment variables if available
        import os
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_session_token = os.getenv('AWS_SESSION_TOKEN')
        
        # Create boto3 session with credentials
        try:
            if aws_access_key and aws_secret_key:
                # Use credentials from environment variables
                session_kwargs = {
                    'aws_access_key_id': aws_access_key,
                    'aws_secret_access_key': aws_secret_key,
                    'region_name': region
                }
                
                # Add session token if available (for temporary credentials)
                if aws_session_token:
                    session_kwargs['aws_session_token'] = aws_session_token
                
                session = boto3.Session(**session_kwargs)
                logger.info("✅ Successfully created AWS session with environment credentials")
                
            else:
                # Try default credential chain
                session = boto3.Session(region_name=region)
                logger.info("✅ Successfully created AWS session with default credentials")
            
            # Test the session by creating a client
            test_client = session.client('sts')
            test_client.get_caller_identity()
            logger.info("✅ AWS credentials validated successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create AWS session: {e}")
            logger.info("Trying to create session without region...")
            try:
                if aws_access_key and aws_secret_key:
                    # Use credentials from environment variables without region
                    session_kwargs = {
                        'aws_access_key_id': aws_access_key,
                        'aws_secret_access_key': aws_secret_key
                    }
                    
                    # Add session token if available
                    if aws_session_token:
                        session_kwargs['aws_session_token'] = aws_session_token
                    
                    session = boto3.Session(**session_kwargs)
                else:
                    session = boto3.Session()
                
                test_client = session.client('sts')
                test_client.get_caller_identity()
                logger.info("✅ Successfully created AWS session without region")
            except Exception as e2:
                logger.error(f"❌ Failed to create AWS session without region: {e2}")
                raise Exception(f"Could not create AWS session. Please check your AWS credentials. Error: {e}")
        
        self.athena_client = session.client('athena')
        self.s3_client = session.client('s3')
        self.database = database
        # Updated to use raw bucket for output
        self.output_location = 's3://insightflow-dev-clean-bucket/athena-results/'
        self.cache_location = 's3://insightflow-dev-clean-bucket/athena-cache/'
        
        # Test the connection
        try:
            # Test if we can access the S3 bucket
            self.s3_client.head_bucket(Bucket='insightflow-dev-clean-bucket')
            logger.info(f"✅ Successfully connected to S3 bucket: insightflow-dev-clean-bucket")
        except Exception as e:
            logger.warning(f"⚠️ Could not access S3 bucket: {e}")
            # Try to create the directories if they don't exist
            try:
                self.s3_client.put_object(
                    Bucket='insightflow-dev-clean-bucket',
                    Key='athena-results/'
                )
                self.s3_client.put_object(
                    Bucket='insightflow-dev-clean-bucket',
                    Key='athena-cache/'
                )
                logger.info("✅ Created athena-results and athena-cache directories in S3 bucket")
            except Exception as create_error:
                logger.error(f"❌ Could not create directories: {create_error}")
    
    def _generate_query_hash(self, query: str) -> str:
        """Generate a hash for the query to use as cache key"""
        return hashlib.md5(query.encode()).hexdigest()
    
    def _get_cached_result(self, query_hash: str) -> Optional[pd.DataFrame]:
        """Check if cached result exists and return it"""
        try:
            cache_key = f"athena-cache/{query_hash}.parquet"
            
            # Check if cached file exists
            try:
                self.s3_client.head_object(Bucket='insightflow-dev-clean-bucket', Key=cache_key)
                logger.info(f"📋 Found cached result for query hash: {query_hash}")
            except:
                logger.info(f"📋 No cached result found for query hash: {query_hash}")
                return None
            
            # Download and load cached result
            response = self.s3_client.get_object(Bucket='insightflow-dev-clean-bucket', Key=cache_key)
            
            # Read the entire content into memory first
            parquet_content = response['Body'].read()
            
            # Create a BytesIO object for pandas to read
            parquet_buffer = io.BytesIO(parquet_content)
            
            # Read the parquet file
            df = pd.read_parquet(parquet_buffer)
            logger.info(f"✅ Loaded cached result with {len(df)} rows")
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to load cached result: {e}")
            return None
    
    def _save_result_to_cache(self, query_hash: str, df: pd.DataFrame) -> bool:
        """Save query result to cache"""
        try:
            cache_key = f"athena-cache/{query_hash}.parquet"
            
            # Convert DataFrame to parquet bytes using BytesIO
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            parquet_content = parquet_buffer.getvalue()
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket='insightflow-dev-clean-bucket',
                Key=cache_key,
                Body=parquet_content
            )
            
            logger.info(f"💾 Saved result to cache: {cache_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save result to cache: {e}")
            return False
    
    def execute_query(self, query: str, query_name: str = "analysis", use_cache: bool = True, max_rows: int = 1000) -> pd.DataFrame:
        """Execute Athena query and return results as DataFrame with optional caching and pagination"""
        try:
            logger.info(f"Executing Athena query: {query_name}")
            
            # Generate query hash for caching
            query_hash = self._generate_query_hash(query)
            
            # Check cache first if enabled
            if use_cache:
                cached_result = self._get_cached_result(query_hash)
                if cached_result is not None and len(cached_result) > 0:
                    logger.info(f"🎯 Returning cached result for {query_name} ({len(cached_result)} rows)")
                    return cached_result
            
            # Start query execution using default workgroup
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Query execution ID: {query_execution_id}")
            
            # Wait for query to complete
            while True:
                query_status = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status']['State']
                
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
                time.sleep(2)
            
            if query_status == 'SUCCEEDED':
                # Get results with pagination if needed
                all_data_rows = []
                columns = None
                next_token = None
                
                while True:
                    if next_token:
                        results = self.athena_client.get_query_results(
                            QueryExecutionId=query_execution_id,
                            NextToken=next_token
                        )
                    else:
                        results = self.athena_client.get_query_results(
                            QueryExecutionId=query_execution_id
                        )
                    
                    # Process this batch
                    if 'ResultSet' in results and 'Rows' in results['ResultSet']:
                        rows = results['ResultSet']['Rows']
                        
                        if len(rows) > 0:
                            # Get column names from header row (only once)
                            if columns is None:
                                columns = [col['VarCharValue'] for col in rows[0]['Data']]
                            
                            # Get data rows (skip header row)
                            for row in rows[1:]:
                                data_row = []
                                for col in row['Data']:
                                    value = col.get('VarCharValue', '')
                                    data_row.append(value)
                                all_data_rows.append(data_row)
                    
                    # Check if there are more results and we haven't reached max_rows
                    next_token = results.get('NextToken')
                    if not next_token or len(all_data_rows) >= max_rows:
                        break
                
                # Create DataFrame from all collected data
                if all_data_rows and columns:
                    df = pd.DataFrame(all_data_rows, columns=columns)
                    
                    # Convert numeric columns
                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col], errors='ignore')
                        except:
                            pass
                else:
                    df = pd.DataFrame()
                
                logger.info(f"Query completed successfully. Returned {len(df)} rows")
                
                # Save to cache if enabled and we have results
                if use_cache and len(df) > 0:
                    self._save_result_to_cache(query_hash, df)
                
                return df
            else:
                error_msg = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Query failed: {error_msg}")
                
        except Exception as e:
            logger.error(f"Error executing Athena query: {e}")
            raise
    
    def create_materialized_view(self, query: str, view_name: str, refresh: bool = False) -> bool:
        """Create a materialized view (CTAS - Create Table As Select) for faster querying"""
        try:
            # Check if view exists and if we need to refresh
            if not refresh:
                try:
                    # Check if view exists and is valid
                    check_query = f"SELECT COUNT(*) FROM {self.database}.{view_name} LIMIT 1"
                    self.execute_query(check_query, f"check_{view_name}")
                    logger.info(f"📋 Materialized view {view_name} already exists and is valid")
                    return True
                except:
                    logger.info(f"📋 Materialized view {view_name} does not exist, creating...")
            
            # Only drop and recreate when refresh=True or view doesn't exist
            if refresh:
                self.drop_materialized_view(view_name)
                logger.info(f"🔄 Refreshing materialized view: {view_name}")
            
            # Create materialized view using CTAS
            ctas_query = f"""
            CREATE TABLE {self.database}.{view_name}
            WITH (
                format = 'PARQUET',
                external_location = 's3://insightflow-dev-clean-bucket/materialized-views/{view_name}/'
            )
            AS {query}
            """
            
            logger.info(f"🔨 Creating materialized view: {view_name}")
            self.execute_query(ctas_query, f"create_{view_name}", use_cache=False)
            
            logger.info(f"✅ Materialized view {view_name} created successfully with latest query")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create materialized view {view_name}: {e}")
            return False
    
    def query_materialized_view(self, view_name: str, additional_filters: str = "", max_rows: int = 5000) -> pd.DataFrame:
        """Query a materialized view for faster results"""
        try:
            query = f"SELECT * FROM {self.database}.{view_name}"
            if additional_filters:
                query += f" WHERE {additional_filters}"
            
            # Add LIMIT clause to respect max_rows parameter
            query += f" LIMIT {max_rows}"
            
            return self.execute_query(query, f"query_{view_name}", use_cache=True, max_rows=max_rows)
            
        except Exception as e:
            logger.error(f"❌ Failed to query materialized view {view_name}: {e}")
            raise
    
    def refresh_materialized_view(self, view_name: str, max_rows: int = 5000) -> bool:
        """Refresh a materialized view by recreating it"""
        try:
            # Get the original query from metadata (you might want to store this)
            logger.info(f"🔄 Refreshing materialized view: {view_name}")
            
            # For now, we'll recreate the view based on common patterns
            if view_name == "product_affinity_view":
                query = self._get_product_affinity_query(max_rows=max_rows)
            elif view_name == "customer_journey_view":
                query = self._get_customer_journey_query(max_rows=max_rows)
            elif view_name == "churn_analysis_view":
                query = self._get_churn_analysis_query(max_rows=max_rows)
            elif view_name == "lifetime_value_view":
                query = self._get_lifetime_value_query(max_rows=max_rows)
            else:
                raise ValueError(f"Unknown materialized view: {view_name}")
            
            return self.create_materialized_view(query, view_name, refresh=True)
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh materialized view {view_name}: {e}")
            return False
    
    def _get_product_affinity_query(self, top_products: int = 20, departments: list = None, max_rows: int = 5000, limit_pairs: Optional[int] = None) -> str:
        """Get the product affinity query that matches in-memory approach with optional department filtering.

        limit_pairs: if provided, caps the final number of product pairs returned. This
        should typically be top_products * (top_products - 1) / 2 to ensure the heatmap
        can be constructed with a complete N x N grid.
        """
        
        # Build department filter
        department_filter = ""
        if departments and "All Departments" not in departments:
            # Create a list of department names for filtering
            dept_names = [f"'{dept}'" for dept in departments if dept != "All Departments"]
            if dept_names:
                department_filter = f"""
                AND p.department_id IN (
                    SELECT department_id 
                    FROM {self.database}.after_clean_departments 
                    WHERE department IN ({','.join(dept_names)})
                )
                """
        
        return f"""
        WITH combined_order_products AS (
            SELECT * FROM {self.database}.after_clean_order_products_prior
            UNION ALL
            SELECT * FROM {self.database}.after_clean_order_products_train
        ),
        top_products AS (
            SELECT product_id
            FROM (
                SELECT op.product_id, COUNT(*) as frequency
                FROM combined_order_products op
                JOIN {self.database}.after_clean_products p ON CAST(op.product_id AS BIGINT) = CAST(p.product_id AS BIGINT)
                WHERE 1=1 {department_filter}
                GROUP BY op.product_id
                ORDER BY frequency DESC
                LIMIT {top_products}
            )
        ),
        filtered_order_products AS (
            SELECT op.*
            FROM combined_order_products op
            JOIN top_products tp ON op.product_id = tp.product_id
        ),
        product_pairs AS (
            -- Match in-memory approach: count unique pairs per order
            SELECT 
                CAST(op1.product_id AS BIGINT) as product1_id,
                CAST(op2.product_id AS BIGINT) as product2_id,
                COUNT(*) as pair_count  -- Count individual pairs, not orders
            FROM filtered_order_products op1
            JOIN filtered_order_products op2 
                ON op1.order_id = op2.order_id 
                AND CAST(op1.product_id AS BIGINT) < CAST(op2.product_id AS BIGINT)
            GROUP BY CAST(op1.product_id AS BIGINT), CAST(op2.product_id AS BIGINT)
        )
        SELECT 
            pp.product1_id,
            p1.product_name as product1_name,
            p1.department_id as product1_department_id,
            d1.department as product1_department,
            pp.product2_id,
            p2.product_name as product2_name,
            p2.department_id as product2_department_id,
            d2.department as product2_department,
            pp.pair_count
        FROM product_pairs pp
        JOIN {self.database}.after_clean_products p1 ON pp.product1_id = CAST(p1.product_id AS BIGINT)
        JOIN {self.database}.after_clean_products p2 ON pp.product2_id = CAST(p2.product_id AS BIGINT)
        JOIN {self.database}.after_clean_departments d1 ON p1.department_id = d1.department_id
        JOIN {self.database}.after_clean_departments d2 ON p2.department_id = d2.department_id
        ORDER BY pp.pair_count DESC
        LIMIT {limit_pairs if limit_pairs is not None else max_rows}
        """
    
    def _get_customer_journey_query(self, max_rows: int = 5000) -> str:
        """Get the customer journey query for materialized view"""
        return f"""
        WITH combined_order_products AS (
            SELECT * FROM {self.database}.after_clean_order_products_prior
            UNION ALL
            SELECT * FROM {self.database}.after_clean_order_products_train
        ),
        order_metrics AS (
            SELECT 
                o.order_id,
                o.user_id,
                COUNT(*) as items_in_order,
                SUM(CASE WHEN CAST(op.reordered AS INTEGER) = 1 THEN 1 ELSE 0 END) as reordered_items,
                MAX(CAST(op.add_to_cart_order AS INTEGER)) as order_size
            FROM {self.database}.after_clean_orders o
            JOIN combined_order_products op ON o.order_id = op.order_id
            GROUP BY o.order_id, o.user_id
        ),
        customer_metrics AS (
            SELECT 
                user_id,
                COUNT(DISTINCT order_id) as total_orders,
                SUM(items_in_order) as total_items,
                SUM(reordered_items) as total_reordered,
                AVG(order_size) as avg_order_size
            FROM order_metrics
            GROUP BY user_id
        ),
        global_metrics AS (
            SELECT
                COUNT(DISTINCT om.order_id) AS total_orders,
                COUNT(DISTINCT om.user_id) AS total_customers,
                SUM(CASE WHEN cm.total_orders = 1 THEN 1 ELSE 0 END) AS first_time_customers,
                SUM(CASE WHEN cm.total_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
                SUM(om.items_in_order) AS total_items,
                SUM(om.reordered_items) AS reordered_items,
                SUM(om.items_in_order) - SUM(om.reordered_items) AS new_items,
                SUM(CASE WHEN om.order_size <= 5 THEN 1 ELSE 0 END) AS small_orders,
                SUM(CASE WHEN om.order_size > 5 AND om.order_size <= 15 THEN 1 ELSE 0 END) AS medium_orders,
                SUM(CASE WHEN om.order_size > 15 THEN 1 ELSE 0 END) AS large_orders
            FROM order_metrics om
            JOIN customer_metrics cm ON om.user_id = cm.user_id
        )
        SELECT * FROM global_metrics
        LIMIT {max_rows}
        """

    def _get_lifetime_value_query(self, max_rows: int = 5000) -> str:
        """Get the lifetime value query for materialized view"""
        return f"""
        WITH combined_order_products AS (
            SELECT * FROM {self.database}.after_clean_order_products_prior
            UNION ALL
            SELECT * FROM {self.database}.after_clean_order_products_train
        ),
        customer_metrics AS (
            SELECT 
                o.user_id,
                COUNT(DISTINCT op.order_id) as total_orders,
                COUNT(*) as total_items,
                SUM(CASE WHEN CAST(op.reordered AS INTEGER) = 1 THEN 1 ELSE 0 END) as total_reorders,
                AVG(CAST(op.add_to_cart_order AS DOUBLE)) as avg_order_size,
                MAX(CAST(o.order_number AS INTEGER)) as max_order_number
            FROM combined_order_products op
            JOIN {self.database}.after_clean_orders o ON op.order_id = o.order_id
            GROUP BY o.user_id
        )
        SELECT 
            user_id,
            total_orders,
            total_items,
            total_reorders,
            avg_order_size,
            max_order_number,
            CASE 
                WHEN total_orders >= 10 THEN 'High Value'
                WHEN total_orders >= 5 THEN 'Medium Value'
                ELSE 'Low Value'
            END as customer_segment
        FROM customer_metrics
        ORDER BY total_items DESC
        LIMIT {max_rows}
        """

    def _get_churn_analysis_query(self, max_rows: int = 5000) -> str:
        """Get the churn analysis query for materialized view"""
        return f"""
        WITH user_orders AS (
            SELECT 
                user_id,
                order_id,
                days_since_prior_order
            FROM {self.database}.after_clean_orders
            WHERE CAST(order_number AS INTEGER) != 1
        ),
        user_metrics AS (
            SELECT 
                user_id,
                COUNT(DISTINCT order_id) as total_orders,
                AVG(CAST(days_since_prior_order AS DOUBLE)) AS avg_days_between,
                MAX(CAST(days_since_prior_order AS DOUBLE)) AS max_days_between,
                STDDEV(CAST(days_since_prior_order AS DOUBLE)) AS std_days_between
            FROM user_orders
            GROUP BY user_id
        )
        SELECT 
            user_id,
            total_orders,
            COALESCE(avg_days_between, 0) AS avg_days_between,
            COALESCE(max_days_between, 0) AS max_days_between,
            COALESCE(std_days_between, 0) AS std_days_between
        FROM user_metrics
        LIMIT {max_rows}
        """
    
    def _convert_results_to_dataframe(self, results: Dict[str, Any]) -> pd.DataFrame:
        """Convert Athena results to pandas DataFrame"""
        if 'ResultSet' not in results or 'Rows' not in results['ResultSet']:
            return pd.DataFrame()
        
        rows = results['ResultSet']['Rows']
        if len(rows) < 2:  # No data rows
            return pd.DataFrame()
        
        # Get column names from header row
        columns = [col['VarCharValue'] for col in rows[0]['Data']]
        
        # Get data rows
        data_rows = []
        for row in rows[1:]:
            data_row = []
            for col in row['Data']:
                value = col.get('VarCharValue', '')
                data_row.append(value)
            data_rows.append(data_row)
        
        df = pd.DataFrame(data_rows, columns=columns)
        
        # Reset index to avoid duplicate index issues
        df = df.reset_index(drop=True)
        
        # Convert numeric columns
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        return df
    
    def create_product_affinity_analysis(self, top_products: int = 20, use_cache: bool = True, max_rows: int = 5000, departments: list = None) -> Tuple[go.Figure, pd.DataFrame]:
        """Create product affinity analysis using Athena with raw data"""
        # Use the updated query method
        limit_pairs = int(top_products * (top_products - 1) / 2)
        query = self._get_product_affinity_query(top_products, departments, max_rows, limit_pairs=limit_pairs)
        
        df = self.execute_query(query, "product_affinity", use_cache=use_cache, max_rows=max_rows)
        
        # Remove any remaining duplicates (just in case)
        df = df.drop_duplicates(subset=['product1_id', 'product2_id'])
        
        # Create visualization with enforced N x N heatmap
        if len(df) > 0:
            fig = self._build_affinity_heatmap(df, top_products, title='Product Affinity Analysis (Raw Data)')
        else:
            # Create empty figure if no data
            fig = go.Figure()
            fig.add_annotation(
                text="No product pairs found. Try increasing top_products or check data availability.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Product Affinity Analysis (Raw Data) - No Data',
                height=600
            )
        
        return fig, df
    
    def create_product_affinity_analysis_fast(self, top_products: int = 20, max_rows: int = 5000, departments: list = None, force_refresh: bool = False) -> Tuple[go.Figure, pd.DataFrame]:
        """Create product affinity analysis using materialized view for faster results"""
        # Parameterize view by N to keep results consistent per requested size
        view_name = f"product_affinity_view_n{top_products}"
        
        # Only force refresh when explicitly requested
        if force_refresh:
            logger.info(f"🔄 Force refreshing {view_name} as requested")
        else:
            logger.info(f"📋 Using existing {view_name} if available")
        
        # If departments are specified, we need to refresh the materialized view or use regular query
        if departments and "All Departments" not in departments:
            logger.info("⚠️ Department filtering requested, using regular query instead of materialized view")
            return self.create_product_affinity_analysis(top_products, use_cache=True, max_rows=max_rows, departments=departments)
        
        # Try to create materialized view with latest query
        limit_pairs = int(top_products * (top_products - 1) / 2)
        if not self.create_materialized_view(self._get_product_affinity_query(top_products, departments, max_rows, limit_pairs=limit_pairs), view_name, refresh=force_refresh):
            logger.warning("⚠️ Failed to create materialized view, falling back to regular query")
            return self.create_product_affinity_analysis(top_products, use_cache=True, max_rows=max_rows, departments=departments)
        
        # Query the materialized view
        query = f"SELECT * FROM {self.database}.{view_name} LIMIT {max_rows}"
        df = self.execute_query(query, "product_affinity_fast", use_cache=True, max_rows=max_rows)
        
        # Create visualization with enforced N x N heatmap
        fig = self._build_affinity_heatmap(df, top_products, title='Product Affinity Analysis (Materialized View)')
        
        return fig, df

    def _build_affinity_heatmap(self, df: pd.DataFrame, top_products: int, title: str) -> go.Figure:
        """Create a consistent N x N heatmap from a product-pair dataframe.

        Ensures axes contain exactly top_products items by selecting the most
        frequent products across all pairs. Missing combinations are filled with 0.
        """
        # Determine top N products by total pair participation
        prod_counts = {}
        for pid_col, name_col in [("product1_id", "product1_name"), ("product2_id", "product2_name")]:
            for pid, name in zip(df[pid_col], df[name_col]):
                prod_counts.setdefault(int(pid), {"name": name, "count": 0})
                prod_counts[int(pid)]["count"] += 1

        # Sort by count desc, then name for stability
        top_ids = [pid for pid, _ in sorted(prod_counts.items(), key=lambda x: (-x[1]["count"], str(x[1]["name"])))]
        top_ids = top_ids[:top_products]
        id_to_name = {pid: prod_counts[pid]["name"] for pid in top_ids}

        # Filter to pairs within top set
        filtered = df[df['product1_id'].astype(int).isin(top_ids) & df['product2_id'].astype(int).isin(top_ids)].copy()

        # Create full matrix with zeros, ensure symmetry positions filled
        top_names = [id_to_name[pid] for pid in top_ids]
        matrix = pd.DataFrame(0, index=top_names, columns=top_names, dtype=int)
        for _, row in filtered.iterrows():
            n1 = id_to_name.get(int(row['product1_id']))
            n2 = id_to_name.get(int(row['product2_id']))
            if n1 is not None and n2 is not None:
                matrix.loc[n1, n2] = int(row['pair_count'])
                matrix.loc[n2, n1] = int(row['pair_count'])

        # Mask diagonal by keeping zeros
        fig = px.imshow(
            matrix.values,
            x=matrix.columns,
            y=matrix.index,
            color_continuous_scale='Blues',
            labels=dict(x='Product', y='Product', color='Pair Count'),
            title=title
        )
        # Force all product names to be shown on both axes
        fig.update_xaxes(
            tickmode='array',
            tickvals=list(matrix.columns),
            ticktext=list(matrix.columns),
            tickangle=45,
            automargin=True
        )
        fig.update_yaxes(
            tickmode='array',
            tickvals=list(matrix.index),
            ticktext=list(matrix.index),
            automargin=True
        )
        fig.update_layout(height=600, margin=dict(l=80, r=20, t=60, b=120))
        return fig
    
    def create_customer_journey_analysis(self, use_cache: bool = True, max_rows: int = 5000) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer journey analysis using Athena with raw data"""
        # Use the updated query method
        query = self._get_customer_journey_query(max_rows)
        
        df = self.execute_query(query, "customer_journey", use_cache=use_cache, max_rows=max_rows)
        
        # Debug: Print data info
        logger.info(f"📊 Customer Journey Raw Data: {len(df)} rows")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Sample data:")
            logger.info(df.head())
        
        # Create visualization
        if len(df) > 0:
            # Check if required columns exist
            required_columns = ['first_time_customers', 'repeat_customers', 'total_items', 
                              'reordered_items', 'small_orders', 'medium_orders', 'large_orders']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"⚠️ Missing columns in customer journey data: {missing_columns}")
                logger.warning(f"📊 Available columns: {list(df.columns)}")
                # Return a simple message instead of crashing
                return self._create_simple_customer_journey_figure(df), df
            
            # This query returns global metrics, so we'll create a different visualization
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Customer Distribution', 'Order Size Distribution', 
                               'Reorder Analysis', 'Customer Journey Overview'),
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "pie"}, {"type": "scatter"}]]
            )
            
            # Customer distribution
            fig.add_trace(go.Bar(
                x=['First Time', 'Repeat'],
                y=[df.iloc[0]['first_time_customers'], df.iloc[0]['repeat_customers']],
                name='Customer Types'
            ), row=1, col=1)
            
            # Order size distribution
            fig.add_trace(go.Bar(
                x=['Small (≤5)', 'Medium (6-15)', 'Large (>15)'],
                y=[df.iloc[0]['small_orders'], df.iloc[0]['medium_orders'], df.iloc[0]['large_orders']],
                name='Order Sizes'
            ), row=1, col=2)
            
            # Reorder analysis
            total_items = df.iloc[0]['total_items']
            reordered_items = df.iloc[0]['reordered_items']
            new_items = df.iloc[0]['new_items']
            
            fig.add_trace(go.Pie(
                labels=['Reordered Items', 'New Items'],
                values=[reordered_items, new_items],
                name='Item Types'
            ), row=2, col=1)
            
            # Customer journey overview
            fig.add_trace(go.Scatter(
                x=['Total Orders', 'Total Customers', 'Total Items'],
                y=[df.iloc[0]['total_orders'], df.iloc[0]['total_customers'], df.iloc[0]['total_items']],
                mode='markers+text',
                text=[df.iloc[0]['total_orders'], df.iloc[0]['total_customers'], df.iloc[0]['total_items']],
                textposition='top center',
                name='Key Metrics'
            ), row=2, col=2)
            
            fig.update_layout(height=600, title_text="Customer Journey Analysis (Raw Data)")
        else:
            # Create empty figure if no data
            fig = go.Figure()
            fig.add_annotation(
                text="No customer journey data found.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Customer Journey Analysis (Raw Data) - No Data',
                height=600
            )
        
        return fig, df
    
    def create_customer_journey_analysis_fast(self, force_refresh: bool = False, max_rows: int = 5000) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer journey analysis using materialized view for faster results"""
        view_name = "customer_journey_view"
        
        # Only force refresh when explicitly requested
        if force_refresh:
            logger.info(f"🔄 Force refreshing {view_name} as requested")
        else:
            logger.info(f"📋 Using existing {view_name} if available")
        
        # Try to create materialized view with latest query
        if not self.create_materialized_view(self._get_customer_journey_query(max_rows), view_name, refresh=force_refresh):
            logger.warning("⚠️ Failed to create materialized view, falling back to regular query")
            return self.create_customer_journey_analysis(use_cache=True, max_rows=max_rows)
        
        # Query the materialized view
        query = f"SELECT * FROM {self.database}.{view_name} LIMIT {max_rows}"
        df = self.execute_query(query, "customer_journey_fast", use_cache=True, max_rows=max_rows)
        
        # Debug: Print data info
        logger.info(f"📊 Customer Journey Fast Data: {len(df)} rows")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Sample data:")
            logger.info(df.head())
        
        # Create visualization
        if len(df) > 0:
            # Check if required columns exist
            required_columns = ['first_time_customers', 'repeat_customers', 'total_items', 
                              'reordered_items', 'small_orders', 'medium_orders', 'large_orders']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"⚠️ Missing columns in customer journey data: {missing_columns}")
                logger.warning(f"📊 Available columns: {list(df.columns)}")
                # Return a simple message instead of crashing
                return self._create_simple_customer_journey_figure(df), df
            
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Customer Distribution', 'Order Size Distribution', 
                               'Reorder Analysis', 'Customer Journey Overview'),
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "pie"}, {"type": "scatter"}]]
            )
            
            # Customer distribution
            fig.add_trace(go.Bar(
                x=['First Time', 'Repeat'],
                y=[df.iloc[0]['first_time_customers'], df.iloc[0]['repeat_customers']],
                name='Customer Types'
            ), row=1, col=1)
            
            # Order size distribution
            fig.add_trace(go.Bar(
                x=['Small (≤5)', 'Medium (6-15)', 'Large (>15)'],
                y=[df.iloc[0]['small_orders'], df.iloc[0]['medium_orders'], df.iloc[0]['large_orders']],
                name='Order Sizes'
            ), row=1, col=2)
            
            # Reorder analysis
            total_items = df.iloc[0]['total_items']
            reordered_items = df.iloc[0]['reordered_items']
            new_items = df.iloc[0]['new_items']
            
            fig.add_trace(go.Pie(
                labels=['Reordered Items', 'New Items'],
                values=[reordered_items, new_items],
                name='Item Types'
            ), row=2, col=1)
            
            # Customer journey overview
            fig.add_trace(go.Scatter(
                x=['Total Orders', 'Total Customers', 'Total Items'],
                y=[df.iloc[0]['total_orders'], df.iloc[0]['total_customers'], df.iloc[0]['total_items']],
                mode='markers+text',
                text=[df.iloc[0]['total_orders'], df.iloc[0]['total_customers'], df.iloc[0]['total_items']],
                textposition='top center',
                name='Key Metrics'
            ), row=2, col=2)
            
            fig.update_layout(height=600, title_text="Customer Journey Analysis (Materialized View)")
        else:
            # Create empty figure if no data
            fig = go.Figure()
            fig.add_annotation(
                text="No customer journey data found.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Customer Journey Analysis (Materialized View) - No Data',
                height=600
            )
        
        return fig, df
    
    def _create_simple_customer_journey_figure(self, df: pd.DataFrame) -> go.Figure:
        """Create a simple customer journey figure when some columns are missing"""
        fig = go.Figure()
        
        # Create a simple bar chart with available data
        available_metrics = []
        available_values = []
        
        for col in df.columns:
            if col in ['total_orders', 'total_customers', 'total_items']:
                available_metrics.append(col.replace('_', ' ').title())
                available_values.append(df.iloc[0][col])
        
        if available_metrics:
            fig.add_trace(go.Bar(
                x=available_metrics,
                y=available_values,
                name='Available Metrics'
            ))
            fig.update_layout(
                title='Customer Journey Analysis - Available Data Only',
                height=400,
                xaxis_title='Metrics',
                yaxis_title='Count'
            )
        else:
            fig.add_annotation(
                text="Limited data available for visualization.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Customer Journey Analysis - Limited Data',
                height=400
            )
        
        return fig
    
    def create_lifetime_value_analysis(self, use_cache: bool = True, max_rows: int = 5000) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer lifetime value analysis using Athena with raw data"""
        # Use the updated query method
        query = self._get_lifetime_value_query(max_rows)
        
        df = self.execute_query(query, "lifetime_value", use_cache=use_cache, max_rows=max_rows)
        
        # Debug: Log the data info to help identify inconsistencies
        logger.info(f"📊 Lifetime Value Normal Data: {len(df)} rows from raw query")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Customer segments found:")
            if 'customer_segment' in df.columns:
                segment_counts = df['customer_segment'].value_counts()
                for segment, count in segment_counts.items():
                    logger.info(f"  - {segment}: {count} customers")
            logger.info(f"📊 Total customers: {len(df)}")
            logger.info(f"📊 Total orders range: {df['total_orders'].min()} - {df['total_orders'].max()}")
        
        # Create visualization
        if len(df) > 0:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Customer Revenue Distribution', 'Frequency vs Order Size', 
                               'Reorder Rate Distribution', 'Customer Value Segments'),
                specs=[[{"type": "histogram"}, {"type": "scatter"}],
                       [{"type": "histogram"}, {"type": "scatter"}]]
            )
            
            # Customer revenue distribution (estimated by total items purchased)
            fig.add_trace(go.Histogram(x=df['total_items'], name='Total Items Purchased'), row=1, col=1)
            
            # Customer frequency vs order size relationship
            fig.add_trace(go.Scatter(
                x=df['total_orders'],
                y=df['avg_order_size'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=df['total_reorders'] / df['total_items'].replace(0, 1),  # reorder rate
                    colorscale='Plasma',
                    showscale=True,
                    colorbar=dict(
                        title="Reorder Rate",
                        x=1.02,
                        xanchor="left",
                        thickness=15,
                        len=0.4
                    )
                ),
                text=df['user_id'],
                name='Frequency vs Order Size'
            ), row=1, col=2)
            
            # Reorder rate distribution
            reorder_rate = df['total_reorders'] / df['total_items'].replace(0, 1)
            fig.add_trace(go.Histogram(x=reorder_rate, name='Reorder Rate'), row=2, col=1)
            
            # Customer segments scatter
            marker_sizes = reorder_rate * 50
            marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
            
            fig.add_trace(go.Scatter(
                x=df['total_orders'],
                y=df['avg_order_size'],
                mode='markers',
                marker=dict(
                    size=marker_sizes,
                    color=reorder_rate.fillna(0),
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        title="Reorder Rate",
                        x=1.02,
                        xanchor="left",
                        thickness=15,
                        len=0.4
                    )
                ),
                text=df['user_id'],
                name='Customer Segments'
            ), row=2, col=2)
            
            fig.update_layout(height=600, title_text="Customer Lifetime Value Analysis")
            
            # Update axes labels for better clarity
            fig.update_xaxes(title_text="Total Items Purchased", row=1, col=1)
            fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
            fig.update_xaxes(title_text="Total Orders", row=1, col=2)
            fig.update_yaxes(title_text="Average Order Size (items)", row=1, col=2)
            fig.update_xaxes(title_text="Reorder Rate", row=2, col=1)
            fig.update_yaxes(title_text="Number of Customers", row=2, col=1)
        else:
            # Create empty figure if no data
            fig = go.Figure()
            fig.add_annotation(
                text="No lifetime value data found.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Customer Lifetime Value Analysis (Raw Data) - No Data',
                height=600
            )
        
        return fig, df
    
    def create_lifetime_value_analysis_fast(self, force_refresh: bool = False, max_rows: int = 5000) -> Tuple[go.Figure, pd.DataFrame]:
        """Create customer lifetime value analysis using materialized view for faster results"""
        view_name = "lifetime_value_view"
        
        # Only force refresh when explicitly requested
        if force_refresh:
            logger.info(f"🔄 Force refreshing {view_name} as requested")
        else:
            logger.info(f"📋 Using existing {view_name} if available")
        
        # Try to create materialized view with latest query
        if not self.create_materialized_view(self._get_lifetime_value_query(max_rows=max_rows), view_name, refresh=force_refresh):
            logger.warning("⚠️ Failed to create materialized view, falling back to regular query")
            return self.create_lifetime_value_analysis(use_cache=True, max_rows=max_rows)
        
        # Query the materialized view
        query = f"SELECT * FROM {self.database}.{view_name} LIMIT {max_rows}"
        df = self.execute_query(query, "lifetime_value_fast", use_cache=True, max_rows=max_rows)
        
        # Debug: Log the data info to help identify inconsistencies
        logger.info(f"📊 Lifetime Value Fast Data: {len(df)} rows from materialized view")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Customer segments found:")
            if 'customer_segment' in df.columns:
                segment_counts = df['customer_segment'].value_counts()
                for segment, count in segment_counts.items():
                    logger.info(f"  - {segment}: {count} customers")
            logger.info(f"📊 Total customers: {len(df)}")
            logger.info(f"📊 Total orders range: {df['total_orders'].min()} - {df['total_orders'].max()}")
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Customer Revenue Distribution', 'Frequency vs Order Size', 
                           'Reorder Rate Distribution', 'Customer Value Segments'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}],
                   [{"type": "histogram"}, {"type": "scatter"}]]
        )
        
        # Customer revenue distribution (estimated by total items purchased)
        fig.add_trace(go.Histogram(x=df['total_items'], name='Total Items Purchased'), row=1, col=1)
        
        # Customer frequency vs order size relationship
        fig.add_trace(go.Scatter(
            x=df['total_orders'],
            y=df['avg_order_size'],
            mode='markers',
            marker=dict(
                size=8,
                color=df['total_reorders'] / df['total_items'].replace(0, 1),  # reorder rate
                colorscale='Plasma',
                showscale=True,
                colorbar=dict(
                    title="Reorder Rate",
                    x=1.02,
                    xanchor="left",
                    thickness=15,
                    len=0.4
                )
            ),
            text=df['user_id'],
            name='Frequency vs Order Size'
        ), row=1, col=2)
        
        # Reorder rate distribution
        reorder_rate = df['total_reorders'] / df['total_items'].replace(0, 1)
        fig.add_trace(go.Histogram(x=reorder_rate, name='Reorder Rate'), row=2, col=1)
        
        # Customer segments scatter
        marker_sizes = reorder_rate * 50
        marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
        
        fig.add_trace(go.Scatter(
            x=df['total_orders'],
            y=df['avg_order_size'],
            mode='markers',
            marker=dict(
                size=marker_sizes,
                color=reorder_rate.fillna(0),
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(
                    title="Reorder Rate",
                    x=1.02,
                    xanchor="left",
                    thickness=15,
                    len=0.4
                )
            ),
            text=df['user_id'],
            name='Customer Segments'
        ), row=2, col=2)
        
        fig.update_layout(height=600, title_text="Customer Lifetime Value Analysis")
        
        # Update axes labels for better clarity
        fig.update_xaxes(title_text="Total Items Purchased", row=1, col=1)
        fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
        fig.update_xaxes(title_text="Total Orders", row=1, col=2)
        fig.update_yaxes(title_text="Average Order Size (items)", row=1, col=2)
        fig.update_xaxes(title_text="Reorder Rate", row=2, col=1)
        fig.update_yaxes(title_text="Number of Customers", row=2, col=1)
        
        return fig, df
    
    def create_churn_analysis(self, use_cache: bool = True, max_rows: int = 5000, force_refresh: bool = False) -> Tuple[go.Figure, pd.DataFrame]:
        """Create churn prediction indicators using Athena with raw data"""
        # Only drop materialized view when force_refresh is requested
        if force_refresh:
            logger.info("🔄 Force refreshing churn analysis - dropping materialized view")
            self.drop_materialized_view("churn_analysis_view")
        else:
            logger.info("📋 Using existing churn analysis data - materialized view preserved")
        
        # Check if materialized view exists and use it for consistency
        view_name = "churn_analysis_view"
        try:
            check_query = f"SELECT COUNT(*) FROM {self.database}.{view_name} LIMIT 1"
            self.execute_query(check_query, f"check_{view_name}")
            logger.info(f"📋 Using existing materialized view {view_name} for consistency")
            
            # Query the materialized view for consistent results
            query = f"SELECT * FROM {self.database}.{view_name} LIMIT {max_rows}"
            # Clear cache if force refresh is requested, then use cache
            if force_refresh:
                self.clear_analysis_cache("churn_analysis")
            df = self.execute_query(query, "churn_analysis", use_cache=True, max_rows=max_rows)
            
        except:
            logger.info(f"📋 Materialized view {view_name} not available, using raw query")
            
            # Use the updated query method
            query = self._get_churn_analysis_query(max_rows)
            
            # Debug: Print the exact query being executed
            logger.info(f"🔍 Executing churn analysis query:")
            logger.info(f"Query: {query}")
            
            # Clear cache for churn analysis to ensure fresh results
            if not use_cache or force_refresh:
                self.clear_analysis_cache("churn_analysis")
            
            # Force fresh query to get clean data
            df = self.execute_query(query, "churn_analysis", use_cache=False, max_rows=max_rows)
        
        # Debug: Log the data info to help identify inconsistencies
        data_source = "materialized view" if 'churn_analysis_view' in str(query) else "raw query"
        logger.info(f"📊 Churn Analysis Normal Data: {len(df)} rows from {data_source}")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Total customers: {len(df)}")
            logger.info(f"📊 Total orders range: {df['total_orders'].min()} - {df['total_orders'].max()}")
            logger.info(f"📊 Days between range: {df['avg_days_between'].min():.2f} - {df['avg_days_between'].max():.2f}")
            logger.info(f"📊 Sample data:")
            logger.info(df.head())
        


            
            # Debug: Check if we have customers with multiple orders
            order_counts = df['total_orders'].value_counts().sort_index()
            print(f"📊 Order Count Distribution:")
            print(order_counts)
            
            # Debug: Check days_since_prior data

            print(f"  - Customers with avg_days_between > 0: {(df['avg_days_between'] > 0).sum()}")
            print(f"  - Customers with max_days_between > 0: {(df['max_days_between'] > 0).sum()}")
            print(f"  - Customers with std_days_between > 0: {(df['std_days_between'] > 0).sum()}")
            
            # Debug: Show actual value ranges
            print(f"📊 Value Ranges:")
            print(f"  - avg_days_between: {df['avg_days_between'].min():.2f} - {df['avg_days_between'].max():.2f}")
            print(f"  - max_days_between: {df['max_days_between'].min():.2f} - {df['max_days_between'].max():.2f}")
            print(f"  - std_days_between: {df['std_days_between'].min():.2f} - {df['std_days_between'].max():.2f}")
            
            # Debug: Show sample of customers with different values
            print(f"📊 Sample Customers with Different Values:")
            sample_df = df[df['avg_days_between'] > 0].head(5)
            if len(sample_df) > 0:
                print(sample_df[['user_id', 'total_orders', 'avg_days_between', 'max_days_between', 'std_days_between']])
            else:
                print("No customers with avg_days_between > 0 found")
            
            # Debug: Show customers with multiple orders
            multi_order_df = df[df['total_orders'] > 1]
            print(f"📊 Customers with Multiple Orders: {len(multi_order_df)}")
            if len(multi_order_df) > 0:
                print(f"  - Order range: {multi_order_df['total_orders'].min()} - {multi_order_df['total_orders'].max()}")
                print(f"  - Days range: {multi_order_df['avg_days_between'].min():.1f} - {multi_order_df['avg_days_between'].max():.1f}")
                print(f"  - Max days range: {multi_order_df['max_days_between'].min():.1f} - {multi_order_df['max_days_between'].max():.1f}")
                print("  - Sample multi-order customers:")
                print(multi_order_df.head(3)[['user_id', 'total_orders', 'avg_days_between', 'max_days_between', 'std_days_between']])
            
            # Debug: Show all order counts
            print(f"📊 All Order Counts Found:")
            all_order_counts = df['total_orders'].value_counts().sort_index()
            print(all_order_counts)
            
            # Debug: Check if we have customers with days data
            customers_with_days = df[df['avg_days_between'] > 0]
            print(f"📊 Customers with Days Data: {len(customers_with_days)}")
            if len(customers_with_days) > 0:
                print(f"  - Days range: {customers_with_days['avg_days_between'].min():.1f} - {customers_with_days['avg_days_between'].max():.1f}")
                print(f"  - Max days range: {customers_with_days['max_days_between'].min():.1f} - {customers_with_days['max_days_between'].max():.1f}")
        
        # Process data to match in-memory version (same as fast method)
        if len(df) > 0:
            churn_indicators = df.fillna(0)
        else:
            churn_indicators = pd.DataFrame()
        
        # Create visualization
        if len(df) > 0:
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Days Between Orders Distribution', 'Churn Risk Indicators'),
                specs=[[{"type": "histogram"}, {"type": "scatter"}]]
            )
            
            # Days between orders
            days_data = churn_indicators['avg_days_between']
            
            # Filter out days = 0 for the distribution
            days_data_filtered = days_data[days_data > 0]
            

            
            fig.add_trace(go.Histogram(x=days_data_filtered, name='Days Between Orders'), row=1, col=1)
            
            # Churn risk scatter - handle NaN values and add jitter for better visualization
            marker_sizes = churn_indicators['std_days_between'] * 10
            marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
            
            # Add small jitter to y-axis to prevent overlapping points
            import numpy as np
            y_values = churn_indicators['max_days_between'].copy()
            if len(y_values) > 0:
                # Add small random jitter to prevent overlapping
                jitter = np.random.normal(0, 0.1, len(y_values))
                y_values = y_values + jitter
            
            fig.add_trace(go.Scatter(
                x=churn_indicators['total_orders'],
                y=y_values,
                mode='markers',
                marker=dict(
                    size=marker_sizes,
                    color=churn_indicators['avg_days_between'].fillna(0),
                    colorscale='RdYlGn_r',
                    showscale=True,
                    colorbar=dict(
                        title="Avg Days Between Orders",
                        x=1.02,
                        xanchor="left",
                        thickness=15,
                        len=0.4
                    )
                ),
                text=churn_indicators['user_id'],
                name='Churn Risk'
            ), row=1, col=2)
            
            fig.update_layout(height=400, title_text="Customer Churn Analysis")
            
            # Add axis labels for better clarity and set x-axis range
            fig.update_xaxes(title_text="Days Between Orders", row=1, col=1)
            fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
            
            # Set proper x-axis range for churn risk indicators
            max_orders = churn_indicators['total_orders'].max() if len(churn_indicators) > 0 else 10
            fig.update_xaxes(
                title_text="Total Orders", 
                row=1, col=2,
                range=[0, max_orders + 1]  # Start from 0 to show churn risk
            )
            fig.update_yaxes(title_text="Max Days Between Orders", row=1, col=2)
        else:
            # Create empty figure if no data
            fig = go.Figure()
            fig.add_annotation(
                text="No churn analysis data found.",
                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(
                title='Customer Churn Analysis - No Data',
                height=400
            )
        
        return fig, churn_indicators
    
    def create_churn_analysis_fast(self, force_refresh: bool = False, max_rows: int = 5000) -> Tuple[go.Figure, pd.DataFrame]:
        """Create churn prediction indicators using materialized view for faster results"""
        view_name = "churn_analysis_view"
        
        # Only force refresh when explicitly requested
        if force_refresh:
            logger.info(f"🔄 Force refreshing {view_name} as requested")
        else:
            logger.info(f"📋 Using existing {view_name} if available")
        
        # Try to create materialized view with latest query
        if not self.create_materialized_view(self._get_churn_analysis_query(max_rows), view_name, refresh=force_refresh):
            logger.warning("⚠️ Failed to create materialized view, falling back to regular query")
            return self.create_churn_analysis(use_cache=True, max_rows=max_rows, force_refresh=force_refresh)
        
        # Clear cache if force refresh is requested to ensure fresh data
        if force_refresh:
            self.clear_analysis_cache("churn_analysis_fast")
        
        # Query the materialized view (always use cache after clearing if needed)
        query = f"SELECT * FROM {self.database}.{view_name} LIMIT {max_rows}"
        df = self.execute_query(query, "churn_analysis_fast", use_cache=True, max_rows=max_rows)
        
        # Debug: Log the data info to help identify inconsistencies
        logger.info(f"📊 Churn Analysis Fast Data: {len(df)} rows from materialized view")
        if len(df) > 0:
            logger.info(f"📈 Columns found: {list(df.columns)}")
            logger.info(f"📊 Total customers: {len(df)}")
            logger.info(f"📊 Total orders range: {df['total_orders'].min()} - {df['total_orders'].max()}")
            logger.info(f"📊 Days between range: {df['avg_days_between'].min():.2f} - {df['avg_days_between'].max():.2f}")
            logger.info(f"📊 Sample data:")
            logger.info(df.head())
        
        # Process data to match in-memory version
        if len(df) > 0:
            churn_indicators = df.fillna(0)
        else:
            churn_indicators = pd.DataFrame()
        
        # Create visualization
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Days Between Orders Distribution', 'Churn Risk Indicators'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}]]
        )
        
        # Days between orders
        days_data = churn_indicators['avg_days_between']
        
        # Filter out days = 0 for the distribution
        days_data_filtered = days_data[days_data > 0]
        
        fig.add_trace(go.Histogram(x=days_data_filtered, name='Days Between Orders Distribution'), row=1, col=1)
        
        # Churn risk scatter - handle NaN values and add jitter for better visualization
        marker_sizes = churn_indicators['std_days_between'] * 10
        marker_sizes = marker_sizes.fillna(5)  # Default size for NaN values
        
        # Add small jitter to y-axis to prevent overlapping points
        y_values = churn_indicators['max_days_between'].copy()
        if len(y_values) > 0:
            # Add small random jitter to prevent overlapping
            jitter = np.random.normal(0, 0.1, len(y_values))
            y_values = y_values + jitter
        
        fig.add_trace(go.Scatter(
            x=churn_indicators['total_orders'],
            y=y_values,
            mode='markers',
            marker=dict(
                size=marker_sizes,
                color=churn_indicators['avg_days_between'].fillna(0),
                colorscale='RdYlGn_r',
                showscale=True,
                colorbar=dict(
                    title="Avg Days Between Orders",
                    x=1.02,
                    xanchor="left",
                    thickness=15,
                    len=0.4
                )
            ),
            text=churn_indicators['user_id'],
            name='Churn Risk'
        ), row=1, col=2)
        
        fig.update_layout(height=400, title_text="Customer Churn Analysis")
        
        # Add axis labels for better clarity and set x-axis range
        fig.update_xaxes(title_text="Days Between Orders", row=1, col=1)
        fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
        
        # Set proper x-axis range for churn risk indicators
        max_orders = churn_indicators['total_orders'].max() if len(churn_indicators) > 0 else 10
        fig.update_xaxes(
            title_text="Total Orders", 
            row=1, col=2,
            range=[0, max_orders + 1]  # Start from 0 to show churn risk
        )
        fig.update_yaxes(title_text="Max Days Between Orders", row=1, col=2)
        
        return fig, churn_indicators
    
    def get_departments(self, use_cache: bool = True) -> pd.DataFrame:
        """Get all departments from the database"""
        query = f"""
        SELECT 
            department_id,
            department
        FROM {self.database}.after_clean_departments
        ORDER BY department
        """
        
        return self.execute_query(query, "departments", use_cache=use_cache)

    def get_data_summary(self, use_cache: bool = True, departments: list = None) -> pd.DataFrame:
        """Get summary statistics of the raw data with optional department filtering"""
        
        # Build department filter
        department_filter = ""
        departments_where_clause = ""
        if departments and "All Departments" not in departments:
            # Create a list of department names for filtering
            dept_names = [f"'{dept}'" for dept in departments if dept != "All Departments"]
            if dept_names:
                department_filter = f"""
                AND p.department_id IN (
                    SELECT department_id 
                    FROM {self.database}.after_clean_departments 
                    WHERE department IN ({','.join(dept_names)})
                )
                """
                departments_where_clause = f"""
                WHERE department IN ({','.join(dept_names)})
                """
        
        query = f"""
        WITH combined_order_products AS (
          SELECT * FROM {self.database}.after_clean_order_products_prior
          UNION ALL
          SELECT * FROM {self.database}.after_clean_order_products_train
        ),
        filtered_order_products AS (
          SELECT op.*
          FROM combined_order_products op
          JOIN {self.database}.after_clean_products p ON CAST(op.product_id AS INTEGER) = CAST(p.product_id AS INTEGER)
          WHERE 1=1 {department_filter}
        ),
        filtered_orders AS (
          SELECT DISTINCT o.*
          FROM {self.database}.after_clean_orders o
          JOIN filtered_order_products op ON o.order_id = op.order_id
        ),
        filtered_products AS (
          SELECT p.*
          FROM {self.database}.after_clean_products p
          WHERE 1=1 {department_filter}
        ),
        filtered_departments AS (
          SELECT d.*
          FROM {self.database}.after_clean_departments d
          {departments_where_clause}
        )
        SELECT 
          'Total Users' as metric,
          COUNT(DISTINCT user_id) as value
        FROM filtered_orders
        UNION ALL
        SELECT 
          'Total Orders' as metric,
          COUNT(DISTINCT order_id) as value
        FROM filtered_order_products
        UNION ALL
        SELECT 
          'Total Products' as metric,
          COUNT(DISTINCT product_id) as value
        FROM filtered_products
        UNION ALL
        SELECT 
          'Total Items' as metric,
          COUNT(*) as value
        FROM filtered_order_products
        UNION ALL
        SELECT 
          'Total Departments' as metric,
          COUNT(DISTINCT department_id) as value
        FROM filtered_departments
        UNION ALL
        SELECT 
          'Total Aisles' as metric,
          COUNT(DISTINCT aisle_id) as value
        FROM {self.database}.after_clean_aisles
        """
        
        return self.execute_query(query, "data_summary", use_cache=use_cache)


    def debug_cache(self, query_hash: str = None):
        """Debug cache functionality"""
        try:
            if query_hash:
                cache_key = f"athena-cache/{query_hash}.parquet"
            else:
                # List all cached files
                response = self.s3_client.list_objects_v2(
                    Bucket='insightflow-dev-clean-bucket',
                    Prefix='athena-cache/'
                )
                cached_files = [obj['Key'] for obj in response.get('Contents', [])]
                print(f"📁 Found {len(cached_files)} cached files:")
                for file in cached_files:
                    print(f"  - {file}")
                return
            
            # Check specific cache file
            try:
                head_response = self.s3_client.head_object(
                    Bucket='insightflow-dev-clean-bucket', 
                    Key=cache_key
                )
                print(f"✅ Cache file exists: {cache_key}")
                print(f"📊 File size: {head_response['ContentLength']} bytes")
                print(f"📅 Last modified: {head_response['LastModified']}")
                
                # Try to read the file
                response = self.s3_client.get_object(
                    Bucket='insightflow-dev-clean-bucket', 
                    Key=cache_key
                )
                parquet_content = response['Body'].read()
                print(f"📄 Content length: {len(parquet_content)} bytes")
                
                # Try to parse as parquet
                parquet_buffer = io.BytesIO(parquet_content)
                df = pd.read_parquet(parquet_buffer)
                print(f"✅ Successfully loaded {len(df)} rows from cache")
                print(f"📊 Columns: {list(df.columns)}")
                
            except Exception as e:
                print(f"❌ Error reading cache file: {e}")
                
        except Exception as e:
            print(f"❌ Error debugging cache: {e}")

    def clear_cache(self):
        """Clear all cached results"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket='insightflow-dev-clean-bucket',
                Prefix='athena-cache/'
            )
            cached_files = [obj['Key'] for obj in response.get('Contents', [])]
            
            for file_key in cached_files:
                self.s3_client.delete_object(
                    Bucket='insightflow-dev-clean-bucket',
                    Key=file_key
                )
                print(f"🗑️ Deleted: {file_key}")
            
            print(f"✅ Cleared {len(cached_files)} cached results")
            
        except Exception as e:
            print(f"❌ Failed to clear cache: {e}")
    
    def clear_analysis_cache(self, analysis_name: str):
        """Clear cache for a specific analysis"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket='insightflow-dev-clean-bucket',
                Prefix=f'athena-cache/{analysis_name}'
            )
            cached_files = [obj['Key'] for obj in response.get('Contents', [])]
            
            for file_key in cached_files:
                self.s3_client.delete_object(
                    Bucket='insightflow-dev-clean-bucket',
                    Key=file_key
                )
                logger.info(f"🗑️ Deleted cache: {file_key}")
            
            logger.info(f"✅ Cleared {len(cached_files)} cached results for {analysis_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to clear cache for {analysis_name}: {e}")

    def diagnose_data_types(self):
        """Diagnose the data types of key columns"""
        try:
            # Test queries to check data types
            test_queries = [
                f"""
                SELECT 
                    'after_clean_orders' as table_name,
                    'user_id' as column_name,
                    typeof(user_id) as data_type
                FROM {self.database}.after_clean_orders 
                LIMIT 1
                """,
                f"""
                SELECT 
                    'after_clean_order_products_prior' as table_name,
                    'reordered' as column_name,
                    typeof(reordered) as data_type,
                    reordered as sample_value
                FROM {self.database}.after_clean_order_products_prior 
                LIMIT 1
                """,
                f"""
                SELECT 
                    'after_clean_order_products_prior' as table_name,
                    'add_to_cart_order' as column_name,
                    typeof(add_to_cart_order) as data_type,
                    add_to_cart_order as sample_value
                FROM {self.database}.after_clean_order_products_prior 
                LIMIT 1
                """,
                f"""
                SELECT 
                    'after_clean_products' as table_name,
                    'product_id' as column_name,
                    typeof(product_id) as data_type,
                    product_id as sample_value
                FROM {self.database}.after_clean_products 
                LIMIT 1
                """,
                f"""
                SELECT 
                    'after_clean_orders' as table_name,
                    'days_since_prior_order' as column_name,
                    typeof(days_since_prior_order) as data_type,
                    days_since_prior_order as sample_value
                FROM {self.database}.after_clean_orders 
                LIMIT 1
                """
            ]
            
            results = []
            for i, query in enumerate(test_queries):
                try:
                    result = self.execute_query(query, f"data_type_check_{i}", use_cache=False)
                    results.append(result)
                    print(f"Query {i+1} result: {result}")
                except Exception as e:
                    print(f"Query {i+1} failed: {e}")
            
            return results
            
        except Exception as e:
            print(f"Error checking data types: {e}")
            return None

    def cache_popular_products(self, top_n: int = 20, cache_json_path: str = "popular_products_cache.json"):
        """Query top N popular products by purchase count and save as a local JSON cache file."""
        query = f'''
            SELECT p.product_id, p.product_name, a.aisle, d.department, COUNT(*) as purchase_count
            FROM {self.database}.after_clean_order_products_prior op
            JOIN {self.database}.after_clean_products p ON CAST(op.product_id AS BIGINT) = CAST(p.product_id AS BIGINT)
            LEFT JOIN {self.database}.after_clean_aisles a ON p.aisle_id = a.aisle_id
            LEFT JOIN {self.database}.after_clean_departments d ON p.department_id = d.department_id
            GROUP BY p.product_id, p.product_name, a.aisle, d.department
            ORDER BY purchase_count DESC
            LIMIT {top_n}
        '''
        logger.info(f"Querying Athena for top {top_n} popular products...")
        df = self.execute_query(query, query_name="popular_products", use_cache=True, max_rows=top_n)
        if df is not None and len(df) > 0:
            # Save as JSON (list of dicts)
            records = df.to_dict(orient="records")
            with open(cache_json_path, "w") as f:
                json.dump(records, f, indent=2)
            logger.info(f"✅ Saved top {top_n} popular products to {cache_json_path}")
        else:
            logger.warning("No popular products found or query failed.")

    def drop_materialized_view(self, view_name: str) -> bool:
        """Drop a materialized view if it exists and clean up S3 directories"""
        try:
            # First, drop the table from Athena
            drop_query = f"DROP TABLE IF EXISTS {self.database}.{view_name}"
            self.execute_query(drop_query, f"drop_{view_name}", use_cache=False)
            logger.info(f"🗑️ Dropped materialized view: {view_name}")
            
            # Then, clean up the S3 directory to prevent HIVE_PATH_ALREADY_EXISTS errors
            try:
                s3_prefix = f"materialized-views/{view_name}/"
                logger.info(f"🧹 Cleaning up S3 directory: s3://insightflow-dev-clean-bucket/{s3_prefix}")
                
                # List and delete all objects in the S3 directory
                response = self.s3_client.list_objects_v2(
                    Bucket='insightflow-dev-clean-bucket',
                    Prefix=s3_prefix
                )
                
                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    if objects_to_delete:
                        self.s3_client.delete_objects(
                            Bucket='insightflow-dev-clean-bucket',
                            Delete={'Objects': objects_to_delete}
                        )
                        logger.info(f"🗑️ Deleted {len(objects_to_delete)} objects from S3 directory")
                    else:
                        logger.info(f"📁 S3 directory {s3_prefix} is already empty")
                else:
                    logger.info(f"📁 S3 directory {s3_prefix} does not exist")
                    
            except Exception as s3_error:
                logger.warning(f"⚠️ Could not clean up S3 directory for {view_name}: {s3_error}")
                # Continue even if S3 cleanup fails
            
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Could not drop materialized view {view_name}: {e}")
            return False

    def get_materialized_view_status(self) -> Dict[str, str]:
        """Get the status of all materialized views"""
        try:
            view_status = {}
            views_to_check = [
                "product_affinity_view", 
                "customer_journey_view", 
                "lifetime_value_view", 
                "churn_analysis_view"
            ]
            
            for view_name in views_to_check:
                try:
                    check_query = f"SELECT COUNT(*) FROM {self.database}.{view_name} LIMIT 1"
                    self.execute_query(check_query, f"status_check_{view_name}", use_cache=False)
                    view_status[view_name] = "✅ Available"
                except:
                    view_status[view_name] = "❌ Not Found"
            
            return view_status
            
        except Exception as e:
            logger.error(f"❌ Failed to check materialized view status: {e}")
            return {}

    def cleanup_all_materialized_view_s3_directories(self) -> bool:
        """Clean up all S3 directories for materialized views to prevent HIVE_PATH_ALREADY_EXISTS errors"""
        try:
            logger.info("🧹 Cleaning up all materialized view S3 directories...")
            
            views_to_cleanup = [
                "product_affinity_view",
                "customer_journey_view", 
                "lifetime_value_view",
                "churn_analysis_view"
            ]
            
            success_count = 0
            for view_name in views_to_cleanup:
                try:
                    s3_prefix = f"materialized-views/{view_name}/"
                    logger.info(f"🧹 Cleaning up S3 directory: s3://insightflow-dev-clean-bucket/{s3_prefix}")
                    
                    # List and delete all objects in the S3 directory
                    response = self.s3_client.list_objects_v2(
                        Bucket='insightflow-dev-clean-bucket',
                        Prefix=s3_prefix
                    )
                    
                    if 'Contents' in response:
                        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                        if objects_to_delete:
                            self.s3_client.delete_objects(
                                Bucket='insightflow-dev-clean-bucket',
                                Delete={'Objects': objects_to_delete}
                            )
                            logger.info(f"🗑️ Deleted {len(objects_to_delete)} objects from {s3_prefix}")
                            success_count += 1
                        else:
                            logger.info(f"📁 S3 directory {s3_prefix} is already empty")
                            success_count += 1
                    else:
                        logger.info(f"📁 S3 directory {s3_prefix} does not exist")
                        success_count += 1
                        
                except Exception as s3_error:
                    logger.error(f"❌ Error cleaning up S3 directory for {view_name}: {s3_error}")
            
            logger.info(f"🧹 S3 cleanup complete: {success_count}/{len(views_to_cleanup)} successful")
            return success_count == len(views_to_cleanup)
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup S3 directories: {e}")
            return False

    def refresh_all_materialized_views(self, max_rows: int = 5000) -> bool:
        """Refresh all materialized views to ensure they use the latest corrected queries"""
        try:
            logger.info("🔄 Refreshing all materialized views to ensure consistency...")
            
            # First, clean up all S3 directories to prevent HIVE_PATH_ALREADY_EXISTS errors
            logger.info("🧹 Cleaning up S3 directories before refresh...")
            self.cleanup_all_materialized_view_s3_directories()
            
            views_to_refresh = [
                "product_affinity_view",
                "customer_journey_view", 
                "lifetime_value_view",
                "churn_analysis_view"
            ]
            
            success_count = 0
            for view_name in views_to_refresh:
                try:
                    if self.refresh_materialized_view(view_name, max_rows=max_rows):
                        success_count += 1
                        logger.info(f"✅ Successfully refreshed {view_name}")
                    else:
                        logger.warning(f"⚠️ Failed to refresh {view_name}")
                except Exception as e:
                    logger.error(f"❌ Error refreshing {view_name}: {e}")
            
            logger.info(f"🔄 Materialized view refresh complete: {success_count}/{len(views_to_refresh)} successful")
            return success_count == len(views_to_refresh)
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh materialized views: {e}")
            return False

def test_athena_analysis():
    """
    Comprehensive test script for Athena analysis functions.
    Run this in your Jupyter notebook to see all the graphs.
    """
    
    print("🚀 Starting Athena Analysis Test (Raw Data)")
    print("=" * 50)
    
    try:
        # Initialize Athena analyzer
        print("📊 Initializing Athena Analyzer...")
        athena_analyzer = AthenaAnalyzer()
        print("✅ Athena Analyzer initialized successfully")
        
        # Clear cache to ensure fresh results
        print("🗑️ Clearing cache for fresh results...")
        athena_analyzer.clear_cache()
        print("✅ Cache cleared successfully")
        
        # Test 1: Product Affinity Analysis
        print("\n🔗 Testing Product Affinity Analysis...")
        try:
            fig1, df1 = athena_analyzer.create_product_affinity_analysis(top_products=20)
            print(f"✅ Product Affinity Analysis completed - {len(df1)} product pairs found")
            print("📊 Displaying Product Affinity Graph:")
            try:
                from IPython.display import display
                display(fig1)
            except ImportError:
                print("⚠️ IPython display not available. Install nbformat>=4.2.0 for better display.")
                fig1.show()
            
            # Show top product pairs
            print("\n🏆 Top 10 Product Pairs:")
            print(df1.head(10)[['product1_name', 'product2_name', 'pair_count']])
            
        except Exception as e:
            print(f"❌ Product Affinity Analysis failed: {e}")
        
        # Test 2: Customer Journey Analysis
        print("\n👥 Testing Customer Journey Analysis...")
        try:
            fig2, df2 = athena_analyzer.create_customer_journey_analysis()
            print(f"✅ Customer Journey Analysis completed - {len(df2)} customers analyzed")
            print("📊 Displaying Customer Journey Graph:")
            try:
                from IPython.display import display
                display(fig2)
            except ImportError:
                print("⚠️ IPython display not available. Install nbformat>=4.2.0 for better display.")
                fig2.show()
            
            # Show customer statistics
            print("\n📈 Customer Statistics:")
            print(f"Total customers: {len(df2)}")
            print(f"Average orders per customer: {df2['total_orders'].mean():.2f}")
            print(f"Average items per customer: {df2['total_items'].mean():.2f}")
            
        except Exception as e:
            print(f"❌ Customer Journey Analysis failed: {e}")
        
        # Test 3: Lifetime Value Analysis
        print("\n💰 Testing Lifetime Value Analysis...")
        try:
            fig3, df3 = athena_analyzer.create_lifetime_value_analysis()
            print(f"✅ Lifetime Value Analysis completed - {len(df3)} customers analyzed")
            print("📊 Displaying Lifetime Value Graph:")
            try:
                from IPython.display import display
                display(fig3)
            except ImportError:
                print("⚠️ IPython display not available. Install nbformat>=4.2.0 for better display.")
                fig3.show()
            
            # Show customer segments
            print("\n🎯 Customer Segments:")
            segment_counts = df3['customer_segment'].value_counts()
            for segment, count in segment_counts.items():
                percentage = (count / len(df3)) * 100
                print(f"  {segment}: {count} customers ({percentage:.1f}%)")
            
        except Exception as e:
            print(f"❌ Lifetime Value Analysis failed: {e}")
        
        # Test 4: Churn Analysis
        print("\n📉 Testing Churn Analysis...")
        try:
            fig4, df4 = athena_analyzer.create_churn_analysis()
            print(f"✅ Churn Analysis completed - {len(df4)} customers analyzed")
            print("📊 Displaying Churn Analysis Graph:")
            try:
                from IPython.display import display
                display(fig4)
            except ImportError:
                print("⚠️ IPython display not available. Install nbformat>=4.2.0 for better display.")
                fig4.show()
            
            # Show churn statistics
            print("\n⚠️ Churn Risk Statistics:")
            print(f"Average days between orders: {df4['avg_days_between'].mean():.1f} days")
            print(f"Max days between orders: {df4['max_days_between'].max():.1f} days")
            print(f"Customers with high churn risk (>14 days avg): {(df4['avg_days_between'] > 14).sum()}")
            
        except Exception as e:
            print(f"❌ Churn Analysis failed: {e}")
        
        print("\n🎉 All tests completed!")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Failed to initialize Athena Analyzer: {e}")
        print("Please check your AWS credentials and S3 bucket access.")

# Individual test functions for specific analysis
def test_product_affinity():
    """Test only product affinity analysis"""
    print("🔗 Testing Product Affinity Analysis...")
    athena_analyzer = AthenaAnalyzer()
    fig, df = athena_analyzer.create_product_affinity_analysis(top_products=20)
    print(f"✅ Found {len(df)} product pairs")
    try:
        from IPython.display import display
        display(fig)
    except ImportError:
        fig.show()
    return fig, df

def test_customer_journey():
    """Test only customer journey analysis"""
    print("👥 Testing Customer Journey Analysis...")
    athena_analyzer = AthenaAnalyzer()
    fig, df = athena_analyzer.create_customer_journey_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    try:
        from IPython.display import display
        display(fig)
    except ImportError:
        fig.show()
    return fig, df

def test_lifetime_value():
    """Test only lifetime value analysis"""
    print("💰 Testing Lifetime Value Analysis...")
    athena_analyzer = AthenaAnalyzer()
    fig, df = athena_analyzer.create_lifetime_value_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    try:
        from IPython.display import display
        display(fig)
    except ImportError:
        fig.show()
    return fig, df

def test_churn_analysis():
    """Test only churn analysis"""
    print("📉 Testing Churn Analysis...")
    athena_analyzer = AthenaAnalyzer()
    fig, df = athena_analyzer.create_churn_analysis()
    print(f"✅ Analyzed {len(df)} customers")
    try:
        from IPython.display import display
        display(fig)
    except ImportError:
        fig.show()
    return fig, df

# Quick connection test
def test_connection():
    """Test Athena connection and basic functionality"""
    print("🔌 Testing Athena Connection...")
    try:
        athena_analyzer = AthenaAnalyzer()
        
        # Test a simple query
        test_query = "SELECT COUNT(*) as total FROM insightflow_imba_clean_data_catalog.after_clean_products LIMIT 1"
        result = athena_analyzer.execute_query(test_query, "test_connection")
        
        print("✅ Connection successful!")
        print(f"📊 Test query result: {result}")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
