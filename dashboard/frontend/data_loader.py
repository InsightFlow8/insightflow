import streamlit as st
import pandas as pd
import requests
import os

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    """Load data from backend API with caching, fallback to local files"""
    # Try different backend URLs in order of preference
    backend_urls = [
        os.getenv("BACKEND_URL", "http://backend:8000"),
        "http://backend:8000"  # Docker service name
    ]
    
    for backend_url in backend_urls:
        try:
            # Try to get data from backend API
            with st.spinner(f"Loading data from {backend_url}..."):
                response = requests.get(f"{backend_url}/data", timeout=600)
                if response.status_code == 200:
                    data = response.json()
                    # Convert dict back to DataFrames
                    df = pd.DataFrame(data['df'])
                    orders = pd.DataFrame(data['orders'])
                    products = pd.DataFrame(data['products'])
                    departments = pd.DataFrame(data['departments'])
                    aisles = pd.DataFrame(data['aisles'])
                    # st.success(f"✅ Data loaded from {backend_url}")
                    return df, orders, products, departments, aisles
                else:
                    st.warning(f"Backend API {backend_url} returned status {response.status_code}")
        except requests.exceptions.ConnectionError:
            st.warning(f"Cannot connect to {backend_url}")
        except Exception as e:
            st.error(f"Error loading data from {backend_url}: {e}")
    
    # If all API attempts fail, fall back to local files
    st.warning("All API attempts failed, falling back to local files")
    return load_data_from_local_files()


def load_data_from_local_files():
    """Fallback: Load data from local CSV files"""
    # Use imba_data directory - check if running in Docker container first
    if os.path.exists("/app/imba_data"):
        data_path = "/app/imba_data"
    else:
        data_path = "../imba_data"
    
    try:
        with st.spinner("Loading data from local files..."):
            # Define file paths based on actual files in imba_data/
            orders_path = os.path.join(data_path, "orders.csv")
            products_path = os.path.join(data_path, "products.csv")
            departments_path = os.path.join(data_path, "departments.csv")
            aisles_path = os.path.join(data_path, "aisles.csv")
            order_products_prior_path = os.path.join(data_path, "order_products__prior.csv.gz")
            order_products_train_path = os.path.join(data_path, "order_products__train.csv.gz")
            
            # Check if files exist
            required_files = [orders_path, products_path, departments_path, aisles_path, 
                             order_products_prior_path, order_products_train_path]
            
            missing_files = [f for f in required_files if not os.path.exists(f)]
            if missing_files:
                st.error(f"Missing data files: {', '.join(missing_files)}")
                raise FileNotFoundError(f"Missing data files: {missing_files}")
            
            # Load all CSV files
            orders = pd.read_csv(orders_path)
            products = pd.read_csv(products_path)
            departments = pd.read_csv(departments_path)
            aisles = pd.read_csv(aisles_path)
            try:
                order_products_prior = pd.read_csv(order_products_prior_path, compression='gzip')
            except Exception as e:
                st.warning(f"Failed to load {order_products_prior_path} as gzipped file: {e}")
                st.info("Trying to load as regular CSV file...")
                order_products_prior = pd.read_csv(order_products_prior_path)
            
            try:
                order_products_train = pd.read_csv(order_products_train_path, compression='gzip')
            except Exception as e:
                st.warning(f"Failed to load {order_products_train_path} as gzipped file: {e}")
                st.info("Trying to load as regular CSV file...")
                order_products_train = pd.read_csv(order_products_train_path)
            
            # Filter orders to keep only user_id < 10000 (consistent with backend)
            orders = orders[orders['user_id'] < 10000]
            
            # Get the list of order_ids from filtered orders
            valid_order_ids = orders['order_id'].unique()
            
            # Filter order_products to keep only records where order_id exists in filtered orders
            order_products_prior = order_products_prior[order_products_prior['order_id'].isin(valid_order_ids)]
            order_products_train = order_products_train[order_products_train['order_id'].isin(valid_order_ids)]
            
            # Combine order products data
            order_products = pd.concat([order_products_prior, order_products_train], ignore_index=True)
            
            # Merge all data
            df = order_products.merge(orders, on='order_id', how='left')
            df = df.merge(products, on='product_id', how='left')
            df = df.merge(departments, on='department_id', how='left')
            df = df.merge(aisles, on='aisle_id', how='left')
            
            return df, orders, products, departments, aisles
        
    except Exception as e:
        st.error(f"Error loading data from local files: {e}")
        # Return empty DataFrames with proper structure to prevent errors
        empty_df = pd.DataFrame()
        empty_orders = pd.DataFrame(columns=['order_id', 'user_id', 'order_number', 'order_dow', 'order_hour_of_day', 'days_since_prior_order'])
        empty_products = pd.DataFrame(columns=['product_id', 'product_name', 'aisle_id', 'department_id'])
        empty_departments = pd.DataFrame(columns=['department_id', 'department'])
        empty_aisles = pd.DataFrame(columns=['aisle_id', 'aisle'])
        
        return empty_df, empty_orders, empty_products, empty_departments, empty_aisles

@st.cache_data(ttl=60)  # Cache for 1 minute (temporary for testing)
def get_analysis_result(analysis_type, params=None):
    """Get cached analysis results from backend"""
    backend_url = os.getenv("BACKEND_URL", "http://backend:8000")
    
    try:
        response = requests.post(
            f"{backend_url}/analysis/{analysis_type}",
            json=params or {},
            timeout=60
        )
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Error getting analysis: {e}")
        return None