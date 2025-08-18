import pandas as pd
import os
import logging
import boto3
import io
import hashlib
from typing import Dict, Any, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')
import time

logger = logging.getLogger(__name__)

# Global data storage to avoid multiple loads
_global_data = None

class AthenaDataLoader:
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

# Global Athena loader instance
_athena_loader = None

def get_athena_loader():
    """Get or create the global Athena loader instance"""
    global _athena_loader
    if _athena_loader is None:
        _athena_loader = AthenaDataLoader()
    return _athena_loader

def load_products_for_vector_store():
    """Load products data for vector store using Athena"""
    try:
        athena_loader = get_athena_loader()
        
        # Query to get products with aisles and departments from raw data
        query = f"""
        SELECT 
            p.product_id,
            p.product_name,
            p.aisle_id,
            a.aisle,
            p.department_id,
            d.department
        FROM {athena_loader.database}.after_clean_products p
        LEFT JOIN {athena_loader.database}.after_clean_aisles a ON p.aisle_id = a.aisle_id
        LEFT JOIN {athena_loader.database}.after_clean_departments d ON p.department_id = d.department_id
        ORDER BY p.product_id
        """
        
        logger.info("📦 Loading products data for vector store using Athena...")
        # Use max_rows=50000 to ensure we get all products (less than 50,000 as requested)
        products = athena_loader.execute_query(query, "products_for_vector_store", use_cache=True, max_rows=50000)
        
        
        logger.info(f"✅ Products prepared for vector store: {len(products)} records")
        # print(products[products['product_id'] == 45000])
        return products
        
    except Exception as e:
        logger.error(f"❌ Failed to load products for vector store: {e}")
        raise

def load_data_for_ml_model(batch_size: int = 1000, max_users: int = 20000):
    """Load orders and order_products_prior data for ML model using Athena with pagination"""
    try:
        athena_loader = get_athena_loader()
        
        logger.info(f"📦 Loading data for ML model using Athena (max_users={max_users}, batch_size={batch_size})...")
        
        # First, get all user IDs that we need to process
        users_query = f"""
        SELECT DISTINCT user_id
        FROM {athena_loader.database}.after_clean_orders
        WHERE user_id < {max_users}
        ORDER BY user_id
        """
        
        logger.info("🔍 Getting list of users to process...")
        users_df = athena_loader.execute_query(users_query, "users_for_ml_model", use_cache=True, max_rows=max_users)
        
        if len(users_df) == 0:
            logger.warning("⚠️ No users found in the specified range")
            return pd.DataFrame(), pd.DataFrame()
        
        user_ids = users_df['user_id'].tolist()
        total_users = len(user_ids)
        logger.info(f"📊 Found {total_users} users to process")
        
        # Process users in batches
        all_orders = []
        all_order_products_prior = []
        
        for i in range(0, total_users, batch_size):
            batch_user_ids = user_ids[i:i + batch_size]
            batch_start = i + 1
            batch_end = min(i + batch_size, total_users)
            
            logger.info(f"🔄 Processing batch {batch_start}-{batch_end} of {total_users} users...")
            
            # Create user_id filter for this batch
            user_id_list = ','.join(map(str, batch_user_ids))
            
            # Query orders for this batch
            orders_path = 's3://insightflow-dev-clean-bucket/after-clean/after-MICE/run-20250814-003614/orders_imputed.parquet'
            batch_orders = pd.read_parquet(orders_path)
            batch_orders = batch_orders[batch_orders['user_id'].isin(batch_user_ids)]
            batch_orders = batch_orders.sort_values(by=["user_id", "order_number"])

            
            # Query order_products_prior for this batch (only from raw_order_products_prior table)
            order_products_prior_query = f"""
            WITH selected_orders AS (
                SELECT DISTINCT order_id
                FROM {athena_loader.database}.after_clean_orders
                WHERE user_id IN ({user_id_list})
            )
            SELECT 
                op.order_id,
                op.product_id,
                op.add_to_cart_order,
                op.reordered
            FROM {athena_loader.database}.after_clean_order_products_prior op
            INNER JOIN selected_orders so ON op.order_id = so.order_id
            ORDER BY op.order_id, op.add_to_cart_order
            """
            
            # Execute queries for this batch
            try:
                orders_path = 's3://insightflow-dev-clean-bucket/after-clean/after-MICE/run-20250814-003614/orders_imputed.parquet'
                batch_orders = pd.read_parquet(orders_path)
                batch_orders = batch_orders[batch_orders['user_id'].isin(batch_user_ids)]
                batch_orders = batch_orders.sort_values(by=["user_id", "order_number"])
                 
                batch_order_products_prior = athena_loader.execute_query(
                    order_products_prior_query, 
                    f"order_products_prior_for_ml_model_batch_{batch_start}_{batch_end}", 
                    use_cache=True, 
                    max_rows=batch_size * 1000  # Allow more rows per batch for order products
                )
                
                # Append batch results
                if len(batch_orders) > 0:
                    all_orders.append(batch_orders)
                    logger.info(f"  ✅ Orders batch: {len(batch_orders)} records")
                
                if len(batch_order_products_prior) > 0:
                    all_order_products_prior.append(batch_order_products_prior)
                    logger.info(f"  ✅ Order Products Prior batch: {len(batch_order_products_prior)} records")
                
            except Exception as batch_error:
                logger.error(f"❌ Error processing batch {batch_start}-{batch_end}: {batch_error}")
                continue
        
        # Combine all batches
        if all_orders:
            orders = pd.concat(all_orders, ignore_index=True)
            logger.info(f"📊 Total orders loaded: {len(orders):,} records")
        else:
            orders = pd.DataFrame()
            logger.warning("⚠️ No orders data loaded")
        
        if all_order_products_prior:
            order_products_prior = pd.concat(all_order_products_prior, ignore_index=True)
            logger.info(f"📊 Total order products prior loaded: {len(order_products_prior):,} records")
        else:
            order_products_prior = pd.DataFrame()
            logger.warning("⚠️ No order products prior data loaded")
        
        logger.info(f"✅ ML model data preparation completed:")
        logger.info(f"  - Orders: {len(orders):,} records")
        logger.info(f"  - Order Products Prior: {len(order_products_prior):,} records")
        logger.info(f"  - Unique Users: {orders['user_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Orders: {orders['order_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Products: {order_products_prior['product_id'].nunique() if len(order_products_prior) > 0 else 0:,}")
        
        return orders, order_products_prior
        
    except Exception as e:
        logger.error(f"❌ Failed to load data for ML model: {e}")
        raise

def load_data_for_ml_model_optimized(batch_size: int = 500, max_users: int = 10000):
    """Optimized version that loads data more efficiently with better memory management"""
    try:
        athena_loader = get_athena_loader()
        
        logger.info(f"📦 Loading data for ML model (optimized) - max_users={max_users}, batch_size={batch_size}")
        
        # Get total count first to estimate progress
        count_query = f"""
        SELECT COUNT(DISTINCT user_id) as total_users
        FROM {athena_loader.database}.after_clean_orders
        WHERE user_id < {max_users}
        """
        
        count_result = athena_loader.execute_query(count_query, "user_count", use_cache=True)
        total_users = count_result.iloc[0]['total_users'] if len(count_result) > 0 else 0
        
        if total_users == 0:
            logger.warning("⚠️ No users found in the specified range")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"📊 Total users to process: {total_users}")
        
        # Process in smaller batches for better memory management
        all_orders = []
        all_order_products = []
        processed_users = 0
        
        for offset in range(0, total_users, batch_size):
            batch_start = offset + 1
            batch_end = min(offset + batch_size, total_users)
            
            logger.info(f"🔄 Processing users {batch_start}-{batch_end} of {total_users}...")
            
            # Use LIMIT and OFFSET for pagination
            orders_query = f"""
            SELECT 
                order_id,
                user_id,
                order_number,
                order_dow,
                order_hour_of_day,
                days_since_prior_order
            FROM {athena_loader.database}.after_clean_orders
            WHERE user_id < {max_users}
            ORDER BY user_id, order_number
            LIMIT {batch_size} OFFSET {offset}
            """
            
            # Get order IDs for this batch
            order_ids_query = f"""
            SELECT DISTINCT order_id
            FROM {athena_loader.database}.after_clean_orders
            WHERE user_id < {max_users}
            ORDER BY user_id, order_number
            LIMIT {batch_size} OFFSET {offset}
            """
            
            try:
                # Get orders for this batch
                batch_orders = athena_loader.execute_query(
                    orders_query, 
                    f"orders_batch_{batch_start}_{batch_end}", 
                    use_cache=True, 
                    max_rows=batch_size * 50
                )
                
                if len(batch_orders) > 0:
                    # Get order products for these orders
                    order_ids = batch_orders['order_id'].tolist()
                    order_id_list = ','.join(map(str, order_ids))
                    
                    order_products_query = f"""
                    WITH combined_order_products AS (
                        SELECT * FROM {athena_loader.database}.after_clean_order_products_prior
                        UNION ALL
                        SELECT * FROM {athena_loader.database}.after_clean_order_products_train
                    )
                    SELECT 
                        op.order_id,
                        op.product_id,
                        op.add_to_cart_order,
                        op.reordered
                    FROM combined_order_products op
                    WHERE op.order_id IN ({order_id_list})
                    ORDER BY op.order_id, op.add_to_cart_order
                    """
                    
                    batch_order_products = athena_loader.execute_query(
                        order_products_query, 
                        f"order_products_batch_{batch_start}_{batch_end}", 
                        use_cache=True, 
                        max_rows=batch_size * 500
                    )
                    
                    # Append batch results
                    all_orders.append(batch_orders)
                    all_order_products.append(batch_order_products)
                    
                    processed_users += len(batch_orders['user_id'].unique())
                    logger.info(f"  ✅ Batch completed: {len(batch_orders)} orders, {len(batch_order_products)} order products")
                
            except Exception as batch_error:
                logger.error(f"❌ Error processing batch {batch_start}-{batch_end}: {batch_error}")
                continue
        
        # Combine all batches
        if all_orders:
            orders = pd.concat(all_orders, ignore_index=True)
        else:
            orders = pd.DataFrame()
        
        if all_order_products:
            order_products = pd.concat(all_order_products, ignore_index=True)
        else:
            order_products = pd.DataFrame()
        
        logger.info(f"✅ ML model data preparation completed:")
        logger.info(f"  - Orders: {len(orders):,} records")
        logger.info(f"  - Order Products: {len(order_products):,} records")
        logger.info(f"  - Unique Users: {orders['user_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Orders: {orders['order_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Products: {order_products['product_id'].nunique() if len(order_products) > 0 else 0:,}")
        
        return orders, order_products
        
    except Exception as e:
        logger.error(f"❌ Failed to load data for ML model: {e}")
        raise


# Backward compatibility functions
def load_products_for_vector_store_legacy():
    """Legacy function - now uses Athena"""
    return load_products_for_vector_store()

def load_data_for_ml_model_legacy():
    """Legacy function - now uses Athena"""
    return load_data_for_ml_model()

def test_products_loading():
    """Test function to verify products loading with pagination"""
    try:
        print("🧪 Testing products loading with pagination...")
        products = load_products_for_vector_store()
        print(f"✅ Successfully loaded {len(products)} products")
        print(f"📊 Sample products:")
        print(products.head())
        print(f"📈 Product count range: {products['product_id'].min()} - {products['product_id'].max()}")
        return products
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return None 

def test_pagination_loading():
    """Test the pagination functionality with a small sample"""
    try:
        logger.info("🧪 Testing pagination functionality...")
        
        # Test with a small batch size and limited users
        orders, order_products_prior = load_data_for_ml_model(batch_size=100, max_users=1000)
        
        logger.info("✅ Pagination test completed successfully!")
        logger.info(f"📊 Test Results:")
        logger.info(f"  - Orders: {len(orders):,} records")
        logger.info(f"  - Order Products Prior: {len(order_products_prior):,} records")
        logger.info(f"  - Unique Users: {orders['user_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Orders: {orders['order_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Products: {order_products_prior['product_id'].nunique() if len(order_products_prior) > 0 else 0:,}")
        
        # Verify data integrity
        if len(orders) > 0 and len(order_products_prior) > 0:
            # Check that all order_products_prior have corresponding orders
            order_ids_in_orders = set(orders['order_id'].unique())
            order_ids_in_products = set(order_products_prior['order_id'].unique())
            missing_orders = order_ids_in_products - order_ids_in_orders
            
            if len(missing_orders) == 0:
                logger.info("✅ Data integrity check passed - all order products prior have corresponding orders")
            else:
                logger.warning(f"⚠️ Data integrity issue - {len(missing_orders)} order products prior without corresponding orders")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pagination test failed: {e}")
        return False 

def test_ml_data_loading():
    """Test the data loader specifically for ML model training"""
    try:
        logger.info("🧪 Testing data loader for ML model training...")
        
        # Test with small parameters for faster testing
        orders, order_products_prior = load_data_for_ml_model(batch_size=500, max_users=2000)
        
        logger.info("✅ ML data loading test completed successfully!")
        logger.info(f"📊 ML Data Loader Results:")
        logger.info(f"  - Orders: {len(orders):,} records")
        logger.info(f"  - Order Products Prior: {len(order_products_prior):,} records")
        logger.info(f"  - Unique Users: {orders['user_id'].nunique() if len(orders) > 0 else 0:,}")
        logger.info(f"  - Unique Products: {order_products_prior['product_id'].nunique() if len(order_products_prior) > 0 else 0:,}")
        
        # Test data structure for ML training
        if len(orders) > 0 and len(order_products_prior) > 0:
            # Check required columns for ML training
            required_order_cols = ['order_id', 'user_id']
            required_product_cols = ['order_id', 'product_id']
            
            missing_order_cols = [col for col in required_order_cols if col not in orders.columns]
            missing_product_cols = [col for col in required_product_cols if col not in order_products_prior.columns]
            
            if not missing_order_cols and not missing_product_cols:
                logger.info("✅ All required columns present for ML training")
            else:
                logger.warning(f"⚠️ Missing columns: orders={missing_order_cols}, products={missing_product_cols}")
            
            # Test data merge for ML training
            try:
                merged_data = orders[['order_id', 'user_id']].merge(
                    order_products_prior[['order_id', 'product_id']], 
                    on='order_id'
                )
                logger.info(f"✅ Data merge successful: {len(merged_data):,} user-product interactions")
                
                # Test creating user-product matrix
                user_product_counts = merged_data.groupby(['user_id', 'product_id']).size().reset_index(name='times_purchased')
                logger.info(f"✅ User-product matrix created: {len(user_product_counts):,} unique interactions")
                
                # Test data types for ML training
                logger.info("📊 Data type check:")
                logger.info(f"  - user_id dtype: {orders['user_id'].dtype}")
                logger.info(f"  - product_id dtype: {order_products_prior['product_id'].dtype}")
                logger.info(f"  - order_id dtype: {orders['order_id'].dtype}")
                
                # Check for any null values
                null_users = orders['user_id'].isnull().sum()
                null_products = order_products_prior['product_id'].isnull().sum()
                null_orders = orders['order_id'].isnull().sum()
                
                if null_users == 0 and null_products == 0 and null_orders == 0:
                    logger.info("✅ No null values found in key columns")
                else:
                    logger.warning(f"⚠️ Null values found: users={null_users}, products={null_products}, orders={null_orders}")
                
            except Exception as merge_error:
                logger.error(f"❌ Data merge failed: {merge_error}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ ML data loading test failed: {e}")
        return False 