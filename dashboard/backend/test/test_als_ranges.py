#!/usr/bin/env python3
"""
Test script to analyze ALS score ranges
This will help us understand what the actual ALS prediction scores look like
"""

import logging
import sys
import os
import numpy as np

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ml_model import initialize_ml_model, recommend_for_user, get_user_product_score

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def analyze_als_score_ranges():
    """
    Analyze the actual ranges of ALS prediction scores using Spark ALSModel.
    Only sample the top 5 recommendations for the first 50 users.
    """
    from ml_model import spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()

    logging.info("🔍 Analyzing ALS score ranges...")

    # Get user factors as DataFrame
    user_factors = als_model.userFactors.toPandas()

    # Only sample the top 5 recommendations for the first 50 users
    sample_recommendations = []
    sample_users = user_factors['id'].head(50).tolist()

    for user_id in sample_users:
        recs = recommend_for_user(user_id, N=1, normalize=False)
        for _, score in recs:
            sample_recommendations.append(score)

    sample_recommendations_np = np.array(sample_recommendations)

    if len(sample_recommendations_np) > 0:
        min_rec = float(np.min(sample_recommendations_np))
        max_rec = float(np.max(sample_recommendations_np))
        mean_rec = float(np.mean(sample_recommendations_np))
        std_rec = float(np.std(sample_recommendations_np))
        p1_rec = float(np.percentile(sample_recommendations_np, 1))
        p99_rec = float(np.percentile(sample_recommendations_np, 99))
        logging.info(f"📊 Recommendation ALS Score Analysis:")
        logging.info(f"   Min: {min_rec:.6f}")
        logging.info(f"   Max: {max_rec:.6f}")
        logging.info(f"   Mean: {mean_rec:.6f}")
        logging.info(f"   Std: {std_rec:.6f}")
        logging.info(f"   1st percentile: {p1_rec:.6f}")
        logging.info(f"   99th percentile: {p99_rec:.6f}")
        logging.info(f"   Range: {max_rec - min_rec:.6f}")
        print(f"\nSuggested normalization range for all functions: min={p1_rec:.6f}, max={p99_rec:.6f}")

    logging.info("✅ ALS score range analysis completed")

def test_als_score_ranges():
    """
    Test function to check current ALS score ranges using Spark ALSModel.
    """
    from ml_model import spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()

    logging.info("🧪 Testing ALS score ranges...")

    # Test a grid of user-product pairs
    test_users = [1, 100, 1000, 2000, 10000]
    test_products = [1, 100, 1000, 2000, 10000, 20000]
    test_cases = [(u, p) for u in test_users for p in test_products]

    for user_id, product_id in test_cases:
        raw_score = get_user_product_score(user_id, product_id, normalize=False)
        normalized_score = get_user_product_score(user_id, product_id, normalize=True)
        logging.info(f"User {user_id}, Product {product_id}: Raw={raw_score:.6f}, Normalized={normalized_score:.6f}")

    # Test recommendations
    test_users = [1, 100, 1000]
    for user_id in test_users:
        recs = recommend_for_user(user_id, N=3, normalize=False)
        logging.info(f"User {user_id} raw recommendations: {recs[:3]}")
        recs_norm = recommend_for_user(user_id, N=3, normalize=True)
        logging.info(f"User {user_id} normalized recommendations: {recs_norm[:3]}")

    logging.info("✅ ALS score range testing completed")

def main():
    """Main function to test ALS score ranges"""
    print("🔍 Testing ALS Score Ranges")
    print("=" * 50)
    
    try:
        # Initialize the ML model
        print("📚 Initializing ML model...")
        initialize_ml_model()
        
        print("\n🧪 Running score range analysis...")
        analyze_als_score_ranges()
        
        print("\n🧪 Running specific test cases...")
        test_als_score_ranges()
        
        print("\n✅ Analysis completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 