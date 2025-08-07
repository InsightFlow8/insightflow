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

def verify_model_file(model_path):
    """
    Verify that the model file exists and is valid
    """
    try:
        if not os.path.exists(model_path):
            logger.error(f"❌ Model file not found: {model_path}")
            return False
        
        file_size = os.path.getsize(model_path)
        if file_size == 0:
            logger.error(f"❌ Model file is empty: {model_path}")
            return False
        
        logger.info(f"✅ Model file verified: {model_path} ({file_size} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error verifying model file: {e}")
        return False

def load_als_model():
    """Load ALS model from saved pickle file with enhanced error handling"""
    
    # Check if ALS model pickle exists
    model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
    
    # Verify the model file first
    if not verify_model_file(model_path):
        logger.error("ALS model pickle file not found or invalid. Please ensure the model is downloaded from S3.")
        raise FileNotFoundError(f"als_model.pkl not found or invalid at {model_path}")
    
    # Load ALS model and mappings
    try:
        logger.info(f"📚 Loading ALS model from: {model_path}")
        
        with open(model_path, 'rb') as f:
            als_data = pickle.load(f)
        
        # Verify that all required components are present
        required_keys = ['model', 'user_id_map', 'product_id_map', 'reverse_user_id_map', 'reverse_product_id_map', 'sparse_matrix']
        missing_keys = [key for key in required_keys if key not in als_data]
        
        if missing_keys:
            logger.error(f"❌ Model file is missing required components: {missing_keys}")
            raise ValueError(f"Invalid model file - missing components: {missing_keys}")
        
        # Extract components
        model = als_data['model']
        user_id_map = als_data['user_id_map']
        product_id_map = als_data['product_id_map']
        reverse_user_id_map = als_data['reverse_user_id_map']
        reverse_product_id_map = als_data['reverse_product_id_map']
        sparse_matrix = als_data['sparse_matrix']
        
        # Verify model integrity
        if model is None:
            raise ValueError("Model object is None")
        
        logger.info("✅ Loaded ALS model successfully")
        logger.info(f"📊 Model info: {len(user_id_map)} users, {len(product_id_map)} products")
        logger.info(f"📊 Model factors: {model.user_factors.shape[1]} dimensions")
        
        return model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix
        
    except FileNotFoundError:
        logger.error(f"❌ Model file not found: {model_path}")
        logger.error("💡 In EC2 environment, ensure the model is downloaded from S3 before starting the application")
        raise
    except Exception as e:
        logger.error(f"❌ Error loading ALS model: {e}")
        logger.error("💡 The model file may be corrupted or incompatible")
        raise

def scaled_sigmoid(score, scale=0.5):
    """
    Apply scaled sigmoid transformation to normalize scores
    Args:
        score: Raw ALS score
        scale: Scaling factor to control the sigmoid curve (default 0.5)
    Returns:
        float: Normalized score between 0 and 1
    """
    return 1 / (1 + np.exp(-score / scale))

def robust_normalize(score):
    """
    Robust normalization that can handle any score range
    Uses min-max normalization with actual observed bounds
    """
    # Debug: Log the original score
    logger.info(f"DEBUG: Original score: {score}")
    
    # Define bounds based on actual observed ALS score ranges
    # From analysis: Individual scores range from -0.04 to 0.20
    # Recommendation scores range from 0.025 to 0.86
    # Using the broader recommendation range for consistency
    min_bound = -0.05  # Slightly below observed minimum
    max_bound = 0.90   # Slightly above observed maximum
    
    # Clamp the score to the observed bounds
    clamped_score = max(min_bound, min(max_bound, score))
    
    # Apply min-max normalization
    normalized = (clamped_score - min_bound) / (max_bound - min_bound)
    
    # Ensure the result is between 0 and 1
    normalized = max(0.0, min(1.0, normalized))
    
    logger.info(f"DEBUG: Clamped score: {clamped_score}, Normalized: {normalized}")
    
    return normalized

def normalize_popularity_score(score):
    """
    Normalize popularity scores which have much larger ranges
    Popularity scores are typically in the thousands to tens of thousands
    """
    # Debug: Log the original score
    logger.info(f"DEBUG: Original popularity score: {score}")
    
    # Define bounds for popularity scores based on actual observed ranges
    # From analysis: Min=1.00, Max=43,758.00, Mean=88.86
    min_bound = 1.0
    max_bound = 45000.0  # Slightly above observed maximum
    
    # Clamp the score to the observed bounds
    clamped_score = max(min_bound, min(max_bound, score))
    
    # Apply min-max normalization
    normalized = (clamped_score - min_bound) / (max_bound - min_bound)
    
    # Ensure the result is between 0 and 1
    normalized = max(0.0, min(1.0, normalized))
    
    logger.info(f"DEBUG: Clamped popularity score: {clamped_score}, Normalized: {normalized}")
    
    return normalized

def get_user_product_score(user_id, product_id, normalize=True):
    """
    Get the ALS model score for a specific user-product pair
    
    Args:
        user_id: User ID (int or str)
        product_id: Product ID (int or str)
        normalize: If True, return normalized score (0-1). If False, return raw ALS score
    
    Returns:
        float: Normalized score (0-1) if normalize=True, otherwise raw ALS score
    """
    global model, reverse_user_id_map, reverse_product_id_map, sparse_matrix
    
    if model is None:
        return 0.0
    
    try:
        # Convert to int if needed
        if isinstance(user_id, str):
            user_id = int(user_id.strip().strip("'\""))
        if isinstance(product_id, str):
            product_id = int(product_id.strip().strip("'\""))
        
        # Check if user and product exist in training data
        if user_id not in reverse_user_id_map or product_id not in reverse_product_id_map:
            return 0.0
        
        user_idx = reverse_user_id_map[user_id]
        product_idx = reverse_product_id_map[product_id]
        
        # Get user and product factors
        user_factors = model.user_factors[user_idx]
        product_factors = model.item_factors[product_idx]
        
        # Calculate dot product (similarity score)
        raw_score = np.dot(user_factors, product_factors)
        
        if normalize:
            # Use robust normalization for consistent scoring
            normalized_score = robust_normalize(raw_score)
            return float(normalized_score)
        else:
            return float(raw_score)
        
    except Exception as e:
        logger.error(f"Error getting score for user {user_id}, product {product_id}: {e}")
        return 0.0

def recommend_for_user(user_id, N=10, normalize=True):
    """Calls the ALS model's recommend method to get the top-N recommended products for this user."""
    global model, reverse_user_id_map, product_id_map, sparse_matrix
    
    if model is None:
        logger.warning("ML model not initialized, returning popular products")
        return get_popular_products(N, normalize=normalize)
    
    try:
        # Ensure user_id is an int for lookup
        if isinstance(user_id, str):
            user_id = user_id.strip().strip("'\"")
            user_id = int(user_id)
        
        # Check if user exists in training data
        if user_id not in reverse_user_id_map:
            logger.warning(f"User {user_id} not found in training data, using popular products")
            return get_popular_products(N, normalize=normalize)
        
        user_idx = reverse_user_id_map[user_id]
        
        user_row = sparse_matrix.tocsr()[user_idx]
        recommended = model.recommend(user_idx, user_row, N=N)
        # recommended is a list of (product_idx, score)
        item_indices, scores = recommended

        if normalize:
            # Use robust normalization for consistent scoring
            normalized_scores = []
            logger.info(f"DEBUG: Processing {len(scores)} recommendations for user {user_id}")
            for i, score in enumerate(scores):
                raw_score = float(score)
                logger.info(f"DEBUG: Recommendation {i+1}: Raw score = {raw_score}")
                normalized_score = robust_normalize(raw_score)
                normalized_scores.append(normalized_score)
            
            recommendations = [(product_id_map[pid], float(prob)) for pid, prob in zip(item_indices, normalized_scores)]
        else:
            # Return raw scores
            recommendations = [(product_id_map[pid], float(score)) for pid, score in zip(item_indices, scores)]
        
        logger.info(f"✅ Generated {len(recommendations)} personalized recommendations for user {user_id}")
        return recommendations
        
    except KeyError as e:
        logger.warning(f"User {user_id} not found in model mappings, using popular products")
        return get_popular_products(N, normalize=normalize)
    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}")
        logger.info("Falling back to popular products")
        return get_popular_products(N, normalize=normalize)

def get_popular_products(N=10, normalize=True):
    """Get popular products as fallback recommendations"""
    global product_id_map, sparse_matrix
    
    logger.info("DEBUG: Entering get_popular_products function")
    
    if sparse_matrix is None:
        logger.warning("Sparse matrix not available for popular products")
        return []
    
    try:
        # Calculate product popularity (sum of purchases across all users)
        product_popularity = np.array(sparse_matrix.sum(axis=0)).flatten()
        logger.info(f"DEBUG: Product popularity array shape: {product_popularity.shape}")
        
        # Get top N popular products
        top_product_indices = np.argsort(product_popularity)[-N:][::-1]
        logger.info(f"DEBUG: Top product indices: {top_product_indices}")
        
        # Convert to product IDs and create recommendations
        popular_products = []
        raw_scores = []
        product_ids = []
        
        logger.info(f"DEBUG: Processing {len(top_product_indices)} popular products")
        
        for idx in top_product_indices:
            if product_popularity[idx] > 0:  # Only include products with purchases
                product_id = product_id_map[idx]
                raw_score = float(product_popularity[idx])
                product_ids.append(product_id)
                raw_scores.append(raw_score)
                logger.info(f"DEBUG: Popular product {product_id}: Raw popularity score = {raw_score}")
        
        logger.info(f"DEBUG: Found {len(raw_scores)} products with purchases")
        
        if normalize and len(raw_scores) > 0:
            logger.info("DEBUG: Applying normalization to popular products")
            # Use popularity-specific normalization for consistency
            normalized_scores = []
            for i, score in enumerate(raw_scores):
                logger.info(f"DEBUG: Popular product {i+1}: Raw score = {score}")
                normalized_score = normalize_popularity_score(score)
                logger.info(f"DEBUG: Popular product {i+1}: Normalized score = {normalized_score}")
                normalized_scores.append(normalized_score)
            
            popular_products = [(pid, float(prob)) for pid, prob in zip(product_ids, normalized_scores)]
        else:
            logger.info("DEBUG: Not normalizing popular products")
            popular_products = [(pid, score) for pid, score in zip(product_ids, raw_scores)]
        
        logger.info(f"✅ Generated {len(popular_products)} popular product recommendations")
        return popular_products
        
    except Exception as e:
        logger.error(f"Error getting popular products: {e}")
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
        
        # Check if user exists in training data
        if user_id not in reverse_user_id_map:
            logger.warning(f"User {user_id} not found in training data")
            return []
        
        user_idx = reverse_user_id_map[user_id]
        user_vec = model.user_factors[user_idx]
        all_vecs = model.user_factors

        # Compute cosine similarity
        similarities = all_vecs @ user_vec / (np.linalg.norm(all_vecs, axis=1) * np.linalg.norm(user_vec) + 1e-10)
        similarities[user_idx] = -np.inf
        top_indices = np.argsort(similarities)[-N:][::-1]
        similar_users = [user_id_map[idx] for idx in top_indices]
        return similar_users
    except KeyError as e:
        logger.warning(f"User {user_id} not found in model mappings")
        return []
    except Exception as e:
        logger.error(f"Error getting similar users for user {user_id}: {e}")
        return []

def analyze_als_score_ranges():
    """
    Analyze the actual ranges of ALS prediction scores
    This helps us understand what normalization bounds to use
    """
    global model, reverse_user_id_map, reverse_product_id_map, product_id_map
    
    if model is None:
        logger.warning("Cannot analyze score ranges - model not initialized")
        return
    
    try:
        logger.info("🔍 Analyzing ALS score ranges...")
        
        # Sample some user-product pairs to understand score ranges
        sample_scores = []
        sample_recommendations = []
        
        # Get a sample of users (first 10 users in the mapping)
        sample_users = list(reverse_user_id_map.keys())[:10]
        
        for user_id in sample_users:
            if user_id in reverse_user_id_map:
                user_idx = reverse_user_id_map[user_id]
                
                # Get recommendations for this user
                user_row = sparse_matrix.tocsr()[user_idx]
                recommended = model.recommend(user_idx, user_row, N=20)
                item_indices, scores = recommended
                
                # Store raw scores
                for score in scores:
                    sample_recommendations.append(float(score))
                
                # Sample some individual product scores
                sample_products = list(reverse_product_id_map.keys())[:20]
                for product_id in sample_products:
                    if product_id in reverse_product_id_map:
                        product_idx = reverse_product_id_map[product_id]
                        user_factors = model.user_factors[user_idx]
                        product_factors = model.item_factors[product_idx]
                        score = np.dot(user_factors, product_factors)
                        sample_scores.append(float(score))
        
        if sample_scores:
            min_score = min(sample_scores)
            max_score = max(sample_scores)
            mean_score = np.mean(sample_scores)
            std_score = np.std(sample_scores)
            
            logger.info(f"📊 Individual ALS Score Analysis:")
            logger.info(f"   Min: {min_score:.6f}")
            logger.info(f"   Max: {max_score:.6f}")
            logger.info(f"   Mean: {mean_score:.6f}")
            logger.info(f"   Std: {std_score:.6f}")
            logger.info(f"   Range: {max_score - min_score:.6f}")
        
        if sample_recommendations:
            min_rec = min(sample_recommendations)
            max_rec = max(sample_recommendations)
            mean_rec = np.mean(sample_recommendations)
            std_rec = np.std(sample_recommendations)
            
            logger.info(f"📊 Recommendation ALS Score Analysis:")
            logger.info(f"   Min: {min_rec:.6f}")
            logger.info(f"   Max: {max_rec:.6f}")
            logger.info(f"   Mean: {mean_rec:.6f}")
            logger.info(f"   Std: {std_rec:.6f}")
            logger.info(f"   Range: {max_rec - min_rec:.6f}")
        
        # Also analyze popularity scores
        if sparse_matrix is not None:
            product_popularity = np.array(sparse_matrix.sum(axis=0)).flatten()
            non_zero_popularity = product_popularity[product_popularity > 0]
            
            if len(non_zero_popularity) > 0:
                min_pop = np.min(non_zero_popularity)
                max_pop = np.max(non_zero_popularity)
                mean_pop = np.mean(non_zero_popularity)
                
                logger.info(f"📊 Popularity Score Analysis:")
                logger.info(f"   Min: {min_pop:.2f}")
                logger.info(f"   Max: {max_pop:.2f}")
                logger.info(f"   Mean: {mean_pop:.2f}")
                logger.info(f"   Range: {max_pop - min_pop:.2f}")
        
        logger.info("✅ ALS score range analysis completed")
        
    except Exception as e:
        logger.error(f"Error analyzing ALS score ranges: {e}")

def test_als_score_ranges():
    """
    Test function to check current ALS score ranges
    Call this to see what the actual score ranges are
    """
    global model, reverse_user_id_map, reverse_product_id_map
    
    if model is None:
        logger.warning("Model not initialized")
        return
    
    try:
        logger.info("🧪 Testing ALS score ranges...")
        
        # Test a few specific user-product pairs
        test_cases = [
            (1, 1),   # User 1, Product 1
            (1, 100), # User 1, Product 100
            (100, 1), # User 100, Product 1
            (100, 100), # User 100, Product 100
        ]
        
        for user_id, product_id in test_cases:
            if user_id in reverse_user_id_map and product_id in reverse_product_id_map:
                raw_score = get_user_product_score(user_id, product_id, normalize=False)
                normalized_score = get_user_product_score(user_id, product_id, normalize=True)
                logger.info(f"User {user_id}, Product {product_id}: Raw={raw_score:.6f}, Normalized={normalized_score:.6f}")
            else:
                logger.info(f"User {user_id} or Product {product_id} not in training data")
        
        # Test recommendations
        test_users = [1, 100, 1000]
        for user_id in test_users:
            if user_id in reverse_user_id_map:
                recs = recommend_for_user(user_id, N=3, normalize=False)
                logger.info(f"User {user_id} raw recommendations: {recs[:3]}")
                
                recs_norm = recommend_for_user(user_id, N=3, normalize=True)
                logger.info(f"User {user_id} normalized recommendations: {recs_norm[:3]}")
        
        logger.info("✅ ALS score range testing completed")
        
    except Exception as e:
        logger.error(f"Error testing ALS score ranges: {e}")

def initialize_ml_model():
    """Initialize the ML model - load from S3-downloaded file or train if needed"""
    global model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix
    
    try:
        logger.info("🚀 Initializing ML model...")
        
        # Check if ALS model pickle file exists
        model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
        
        if os.path.exists(model_path):
            file_size = os.path.getsize(model_path)
            logger.info(f"✅ ALS model pickle file found: {model_path} ({file_size} bytes)")
            logger.info("📚 Loading pre-trained model (skipping training)")
        else:
            logger.info("🔄 ALS model pickle not found, will train new model")
            logger.info("💡 Tip: In EC2 environment, ensure the model is downloaded from S3 before starting the application")
        
        # Try to load existing model first (will skip training if successful)
        try:
            logger.info("📚 Loading ALS model...")
            model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix = load_als_model()
            
            # Analyze ALS score ranges for the loaded model
            analyze_als_score_ranges()
            
            logger.info("✅ ML model initialization completed successfully (loaded existing model)")
            return
            
        except FileNotFoundError:
            logger.warning("⚠️ Model file not found, will attempt to train new model")
        except Exception as e:
            logger.error(f"❌ Error loading existing model: {e}")
            logger.warning("⚠️ Will attempt to train new model")
        
        # If loading failed, try to train a new model
        logger.info("🔄 Training new ALS model...")
        train_result = train_als_model()
        
        if train_result is None:
            # Training was skipped because model already exists
            logger.info("✅ Training skipped - model already exists")
        else:
            logger.info("✅ New model trained successfully")
        
        # Load the model (either existing or newly trained)
        logger.info("📚 Loading ALS model...")
        model, user_id_map, product_id_map, reverse_user_id_map, reverse_product_id_map, sparse_matrix = load_als_model()
        
        # Analyze ALS score ranges
        analyze_als_score_ranges()

        logger.info("✅ ML model initialization completed successfully.")
        
    except Exception as e:
        logger.error(f"❌ Fatal error initializing ML model: {e}")
        logger.warning("⚠️ Continuing without ML model - recommendations will be limited")
        logger.warning("💡 Ensure the ALS model file is available in the expected location")
        # Don't raise the exception, just log it and continue
        model = None
        user_id_map = None
        product_id_map = None
        reverse_user_id_map = None
        reverse_product_id_map = None
        sparse_matrix = None

