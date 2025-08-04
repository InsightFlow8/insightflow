import os
import pandas as pd
import numpy as np
import pickle
import logging
import scipy.sparse as sparse
from implicit.als import AlternatingLeastSquares
from data_loader import load_data_for_ml_model

logger = logging.getLogger(__name__)

# Global variables for ML model
model = None
user_id_map = None
product_id_map = None
reverse_user_id_map = None
reverse_product_id_map = None
sparse_matrix = None

def train_als_model():
    """
    Load user-product purchase counts and train ALS model
    Skip training if pickle file already exists
    """
    # Check if ALS model pickle file already exists
    model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
    if os.path.exists(model_path):
        logger.info("ALS model pickle file found, skipping training")
        return None, None, None, None, None, None
    
    logger.info("Loading data for ML model training...")
    
    try:
        # Load data using backend data_loader
        orders, order_products_prior = load_data_for_ml_model()
        
        user_product = (
            orders[['order_id', 'user_id']]
            .merge(order_products_prior[['order_id', 'product_id']], on='order_id')
            .groupby(['user_id', 'product_id'])
            .size()
            .reset_index(name='times_purchased')
        )
        # Create a sparse matrix: rows=users, cols=products, values=times_purchased
        user_ids = user_product['user_id'].astype('category').cat.codes
        product_ids = user_product['product_id'].astype('category').cat.codes

        sparse_matrix = sparse.coo_matrix(
            (user_product['times_purchased'], (user_ids, product_ids))
        )

        # Train ALS model with original parameters
        model = AlternatingLeastSquares(factors=50, regularization=0.01, iterations=15)
        model.fit(sparse_matrix)

        # map between your original user/product IDs and the integer indices used in your ALS model's matrix.
        ## user_id_map[0] gives you the real user ID for matrix row 0.
        ## product_id_map[0] gives you the real product ID for matrix column 0.
        ## reverse_user_id_map[real_user_id] gives you the matrix row index for a given user.
        ## reverse_product_id_map[real_product_id] gives you the matrix column index for a given product.

        user_id_map = dict(enumerate(user_product['user_id'].astype('category').cat.categories))
        product_id_map = dict(enumerate(user_product['product_id'].astype('category').cat.categories))
        reverse_user_id_map = {v: k for k, v in user_id_map.items()}
        reverse_product_id_map = {v: k for k, v in product_id_map.items()}

        # Save ALS model and mappings
        als_data = {
            'model': model,
            'user_id_map': user_id_map,
            'product_id_map': product_id_map,
            'reverse_user_id_map': reverse_user_id_map,
            'reverse_product_id_map': reverse_product_id_map,
            'sparse_matrix': sparse_matrix
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(als_data, f)
        
        logger.info("✅ Saved ALS model and mappings to pickle file")

        return model, user_product, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map

    except Exception as e:
        logger.error(f"Error training ALS model: {e}")
        raise

def load_als_model():
    """Load ALS model from saved pickle file"""
    
    # Check if ALS model pickle exists
    model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
    if not os.path.exists(model_path):
        logger.error("ALS model pickle file not found. Please run train_als_model() first.")
        raise FileNotFoundError("als_model.pkl not found")
    
    # Load ALS model and mappings
    try:
        with open(model_path, 'rb') as f:
            als_data = pickle.load(f)
        
        # Extract components
        model = als_data['model']
        user_id_map = als_data['user_id_map']
        product_id_map = als_data['product_id_map']
        reverse_user_id_map = als_data['reverse_user_id_map']
        reverse_product_id_map = als_data['reverse_product_id_map']
        sparse_matrix = als_data['sparse_matrix']
        
        logger.info("✅ Loaded ALS model successfully")
        return model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix
        
    except Exception as e:
        logger.error(f"Error loading ALS model: {e}")
        raise

def recommend_for_user(user_id, N=10):
    """Calls the ALS model's recommend method to get the top-N recommended products for this user."""
    global model, reverse_user_id_map, product_id_map, sparse_matrix
    
    if model is None:
        logger.warning("ML model not initialized, returning empty recommendations")
        return []
    
    try:
        user_idx = reverse_user_id_map[user_id]
        
        user_row = sparse_matrix.tocsr()[user_idx]
        recommended = model.recommend(user_idx, user_row, N=N)
        # recommended is a list of (product_idx, score)
        item_indices, scores = recommended

        return [(product_id_map[pid], float(score)) for pid, score in zip(item_indices, scores)]
    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}")
        return []

def get_similar_users(user_id, N=5):
    """Return the top-N most similar users to the given user_id."""
    global model, reverse_user_id_map, user_id_map
    
    if model is None:
        logger.warning("ML model not initialized, returning empty similar users")
        return []
    
    try:
        # Ensure user_id is an int for lookup
        if isinstance(user_id, str):
            user_id = user_id.strip().strip("'\"")
            user_id = int(user_id)
        user_idx = reverse_user_id_map[user_id]
        user_vec = model.user_factors[user_idx]
        all_vecs = model.user_factors

        # Compute cosine similarity
        similarities = all_vecs @ user_vec / (np.linalg.norm(all_vecs, axis=1) * np.linalg.norm(user_vec) + 1e-10)
        similarities[user_idx] = -np.inf
        top_indices = np.argsort(similarities)[-N:][::-1]
        similar_users = [user_id_map[idx] for idx in top_indices]
        return similar_users
    except Exception as e:
        logger.error(f"Error getting similar users for user {user_id}: {e}")
        return []

def initialize_ml_model():
    """Initialize the ML model - train if needed, load if exists"""
    global model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix
    
    try:
        logger.info("🚀 Initializing ML model...")
        
        # Check if ALS model pickle file exists
        model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
        if os.path.exists(model_path):
            logger.info("✅ ALS model pickle file found")
        else:
            logger.info("🔄 ALS model pickle not found, will train new model")
        
        # Try to train ALS model (will skip if pickle exists)
        train_result = train_als_model()
        
        # Load ALS model
        logger.info("📚 Loading ALS model...")
        model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix = load_als_model()
        
        logger.info("✅ ML model initialization completed successfully.")
        
    except Exception as e:
        logger.error(f"❌ Fatal error initializing ML model: {e}")
        logger.warning("⚠️ Continuing without ML model - recommendations will be limited")
        # Don't raise the exception, just log it and continue
        model = None
        user_id_map = None
        product_id_map = None
        reverse_user_id_map = None
        reverse_product_id_map = None
        sparse_matrix = None

