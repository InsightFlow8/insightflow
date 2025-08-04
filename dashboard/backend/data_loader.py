import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

# Global data storage to avoid multiple loads
_global_data = None

def load_all_data():
    """Load all data once and cache it globally"""
    global _global_data
    
    if _global_data is not None:
        logger.info("📦 Using cached data")
        return _global_data
    
    # Use imba_data directory - check if running in Docker container first
    if os.path.exists("/app/imba_data"):
        data_path = "/app/imba_data"
    else:
        data_path = "../imba_data"
    
    # Define all file paths
    orders_path = os.path.join(data_path, "orders.csv")
    products_path = os.path.join(data_path, "products.csv")
    departments_path = os.path.join(data_path, "departments.csv")
    aisles_path = os.path.join(data_path, "aisles.csv")
    order_products_prior_path = os.path.join(data_path, "order_products__prior.csv.gz")
    order_products_train_path = os.path.join(data_path, "order_products__train.csv.gz")
    
    logger.info(f"🔄 Loading all data from {data_path} directory...")
    
    try:
        # Load all CSV files
        logger.info("Loading orders.csv...")
        orders = pd.read_csv(orders_path)
        
        logger.info("Loading products.csv...")
        products = pd.read_csv(products_path)
        
        logger.info("Loading departments.csv...")
        departments = pd.read_csv(departments_path)
        
        logger.info("Loading aisles.csv...")
        aisles = pd.read_csv(aisles_path)
        
        logger.info("Loading order_products__prior.csv.gz...")
        try:
            order_products_prior = pd.read_csv(order_products_prior_path, compression='gzip')
        except Exception as e:
            logger.warning(f"Failed to load {order_products_prior_path} as gzipped file: {e}")
            logger.info("Trying to load as regular CSV file...")
            order_products_prior = pd.read_csv(order_products_prior_path)
        
        logger.info("Loading order_products__train.csv.gz...")
        try:
            order_products_train = pd.read_csv(order_products_train_path, compression='gzip')
        except Exception as e:
            logger.warning(f"Failed to load {order_products_train_path} as gzipped file: {e}")
            logger.info("Trying to load as regular CSV file...")
            order_products_train = pd.read_csv(order_products_train_path)
        
        logger.info(f"✅ Successfully loaded all data files:")
        logger.info(f"  - Orders: {len(orders):,} records")
        logger.info(f"  - Products: {len(products):,} records")
        logger.info(f"  - Departments: {len(departments):,} records")
        logger.info(f"  - Aisles: {len(aisles):,} records")
        logger.info(f"  - Order Products Prior: {len(order_products_prior):,} records")
        logger.info(f"  - Order Products Train: {len(order_products_train):,} records")
        
        # Combine order products data
        logger.info("Combining order products data...")
        order_products = pd.concat([order_products_prior, order_products_train], ignore_index=True)
        
        # Cache the data globally
        _global_data = {
            'orders': orders,
            'products': products,
            'departments': departments,
            'aisles': aisles,
            'order_products': order_products
        }
        
        logger.info("✅ All data loaded and cached successfully")
        return _global_data
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def load_products_for_vector_store():
    """Load products data for vector store (uses cached data if available)"""
    data = load_all_data()
    
    # Get products and merge with aisles/departments
    products = data['products'].copy()
    aisles = data['aisles']
    departments = data['departments']
    
    # Limit to 1000 products for development (as per user's change)
    products = products[products['product_id'] < 1000]
    
    logger.info(f"📦 Loading {len(products)} products for vector store...")
    
    # Merge products with aisles and departments
    products = products.merge(aisles, on="aisle_id", how="left")
    products = products.merge(departments, on="department_id", how="left")
    
    logger.info(f"✅ Products prepared for vector store: {len(products)} records")
    return products

def load_data_for_ml_model():
    """Load orders and order_products data for ML model (uses cached data if available)"""
    data = load_all_data()
    
    logger.info("📦 Loading data for ML model...")
    
    orders = data['orders']
    orders = orders[orders['user_id'] < 10000]
    order_ids = orders['order_id'].unique()
    order_products = data['order_products']
    order_products = order_products[order_products['order_id'].isin(order_ids)]
    
    logger.info(f"✅ ML model data prepared:")
    logger.info(f"  - Orders: {len(orders):,} records")
    logger.info(f"  - Order Products: {len(order_products):,} records")
    
    return orders, order_products

def get_all_data_for_frontend():
    """Get all data for frontend analysis (uses cached data if available)"""
    data = load_all_data()
    
    logger.info("📦 Preparing data for frontend analysis...")
    
    try:
        # Get all components
        orders = data['orders']
        products = data['products']
        departments = data['departments']
        aisles = data['aisles']
        order_products = data['order_products']
        
        # Filter orders to keep only user_id < 1000 for performance
        logger.info(f"Filtering orders (original: {len(orders):,} records)")
        orders = orders[orders['user_id'] < 10000]

        logger.info(f"Filtered orders: {len(orders):,} records")
        
        # Get the list of order_ids from filtered orders
        valid_order_ids = orders['order_id'].unique()
        logger.info(f"Valid order IDs: {len(valid_order_ids):,}")
        
        # Filter order_products to keep only records where order_id exists in filtered orders
        logger.info(f"Filtering order products (original: {len(order_products):,} records)")
        order_products = order_products[order_products['order_id'].isin(valid_order_ids)]
        logger.info(f"Filtered order products: {len(order_products):,} records")
        
        # Merge all data for frontend
        logger.info("Merging data...")
        df = order_products.merge(orders, on='order_id', how='left')
        df = df.merge(products, on='product_id', how='left')
        df = df.merge(departments, on='department_id', how='left')
        df = df.merge(aisles, on='aisle_id', how='left')
        
        logger.info(f"✅ Frontend data prepared: {len(df):,} records")
        logger.info(f"  - Unique users: {df['user_id'].nunique():,}")
        logger.info(f"  - Unique orders: {df['order_id'].nunique():,}")
        logger.info(f"  - Unique products: {df['product_id'].nunique():,}")
        logger.info(f"  - Unique departments: {df['department_id'].nunique():,}")
        
        return df, orders, products, departments, aisles
        
    except Exception as e:
        logger.error(f"Error preparing frontend data: {e}")
        raise 