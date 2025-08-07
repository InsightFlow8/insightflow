#!/usr/bin/env python3
"""
Test script to check normalized score distributions
"""

import logging
import sys
import os

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ml_model import initialize_ml_model, get_user_product_score, recommend_for_user, get_popular_products

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_normalized_scores():
    """Test normalized score distributions"""
    print("🔍 Testing Normalized Score Distributions")
    print("=" * 50)
    
    try:
        # Initialize the ML model
        print("📚 Initializing ML model...")
        initialize_ml_model()
        
        print("\n🧪 Testing individual product scores...")
        # Test some individual product scores
        test_cases = [
            (1, 1),
            (1, 100), 
            (1, 1000),
            (100, 1),
            (100, 100),
        ]
        
        individual_scores = []
        for user_id, product_id in test_cases:
            try:
                normalized_score = get_user_product_score(user_id, product_id, normalize=True)
                individual_scores.append(normalized_score)
                print(f"User {user_id}, Product {product_id}: {normalized_score:.3f}")
            except:
                print(f"User {user_id}, Product {product_id}: Not in training data")
        
        print(f"\n📊 Individual Score Stats:")
        if individual_scores:
            print(f"Min: {min(individual_scores):.3f}")
            print(f"Max: {max(individual_scores):.3f}")
            print(f"Mean: {sum(individual_scores)/len(individual_scores):.3f}")
        
        print("\n🧪 Testing recommendation scores...")
        # Test recommendation scores
        test_users = [1, 100, 1000]
        recommendation_scores = []
        
        for user_id in test_users:
            try:
                recs = recommend_for_user(user_id, N=5, normalize=True)
                for product_id, score in recs:
                    recommendation_scores.append(score)
                    print(f"User {user_id}, Product {product_id}: {score:.3f}")
            except:
                print(f"User {user_id}: Not in training data")
        
        print(f"\n📊 Recommendation Score Stats:")
        if recommendation_scores:
            print(f"Min: {min(recommendation_scores):.3f}")
            print(f"Max: {max(recommendation_scores):.3f}")
            print(f"Mean: {sum(recommendation_scores)/len(recommendation_scores):.3f}")
        
        print("\n🧪 Testing popularity scores...")
        # Test popularity scores
        try:
            popular_recs = get_popular_products(N=10, normalize=True)
            popularity_scores = [score for _, score in popular_recs]
            
            print(f"📊 Popularity Score Stats:")
            print(f"Min: {min(popularity_scores):.3f}")
            print(f"Max: {max(popularity_scores):.3f}")
            print(f"Mean: {sum(popularity_scores)/len(popularity_scores):.3f}")
            
            print(f"\nTop 5 Popular Products (normalized scores):")
            for i, (product_id, score) in enumerate(popular_recs[:5], 1):
                print(f"{i}. Product {product_id}: {score:.3f}")
                
        except Exception as e:
            print(f"Error testing popularity scores: {e}")
        
        print("\n✅ Normalized score testing completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(test_normalized_scores()) 