import os
import pandas as pd
import numpy as np
# import pickle
import logging
# import scipy.sparse as sparse
# from implicit.als import AlternatingLeastSquares
# from data_loader import load_data_for_ml_model
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import json
from analysis_athena import AthenaAnalyzer

logger = logging.getLogger(__name__)

# Consistent normalization bounds for all functions
NORM_MIN = 0.0
NORM_MAX = 2.0

def consistent_normalize(score, min_bound=NORM_MIN, max_bound=NORM_MAX):
    clamped = max(min_bound, min(max_bound, score))
    normalized = (clamped - min_bound) / (max_bound - min_bound)
    return max(0.0, min(1.0, normalized))

# Remove all code related to training and pickling

# Global variables for ML model
spark = None
als_model = None

def initialize_ml_model():
    """Initialize the Spark session and load the ALS model from the als_model directory."""
    global spark, als_model
    if spark is None:
        spark = SparkSession.builder.appName("ALSModelBackend").getOrCreate()
    if als_model is None:
        model_path = os.getenv("ALS_MODEL_PATH", "als_model")
        als_model = ALSModel.load(model_path)

def get_user_product_score(user_id, product_id, normalize=True):
    """
    Get the ALS model score for a specific user-product pair using Spark ALSModel.
    """
    global spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()

    df = spark.createDataFrame([(user_id, product_id)], ["user_id", "product_id"])
    prediction = als_model.transform(df).collect()
    if not prediction or prediction[0]['prediction'] is None:
        return 0.0
    score = prediction[0]['prediction']
    if normalize:
        return round(consistent_normalize(score), 3)
    else:
        return round(score, 3)

def get_user_product_scores_batch(user_id, product_ids, normalize=True):
    global spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()
    df = spark.createDataFrame([(user_id, pid) for pid in product_ids], ["user_id", "product_id"])
    predictions = als_model.transform(df).collect()
    results = {}
    for row in predictions:
        pid = row['product_id']
        score = row['prediction'] if row['prediction'] is not None else 0.0
        if normalize:
            score = round(consistent_normalize(score), 3)
        else:
            score = round(score, 3)
        results[pid] = score
    return results

def recommend_for_user(user_id, N=10, normalize=True):
    """
    Get top-N product recommendations for a given user_id using the loaded Spark ALS model.
    Returns a list of (product_id, score) tuples.
    """
    global spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()

    users = spark.createDataFrame([(user_id,)], ["user_id"])
    user_recs = als_model.recommendForUserSubset(users, N)
    recs = user_recs.collect()
    if not recs:
        return []
    recommendations = recs[0]['recommendations']
    if normalize:
        return [(r['product_id'], round(consistent_normalize(r['rating']), 3)) for r in recommendations]
    else:
        return [(r['product_id'], round(r['rating'], 3)) for r in recommendations]

POPULAR_PRODUCTS_CACHE = os.path.join(os.path.dirname(__file__), "popular_products_cache.json")

def get_popular_products(N=10, normalize=True):
    """
    Get popular products as fallback recommendations.
    Prefer using cached purchase counts from Athena; fall back to ALS-based proxy if needed.
    """
    # Try to load from cache
    try:
        if os.path.exists(POPULAR_PRODUCTS_CACHE):
            with open(POPULAR_PRODUCTS_CACHE, "r") as f:
                records = json.load(f)
            if records:
                # Sort by purchase_count descending, take top N
                sorted_records = sorted(records, key=lambda x: x.get("purchase_count", 0), reverse=True)[:N]
                # Always return purchase_count directly (do not normalize)
                return [(r["product_id"], r["purchase_count"]) for r in sorted_records]
    except Exception as e:
        logger.warning(f"Failed to load or parse popular products cache: {e}")
    # If cache is missing or empty, try to generate it
    try:
        analyzer = AthenaAnalyzer()
        analyzer.cache_popular_products(top_n=N, cache_json_path=POPULAR_PRODUCTS_CACHE)
        # Try loading again
        if os.path.exists(POPULAR_PRODUCTS_CACHE):
            with open(POPULAR_PRODUCTS_CACHE, "r") as f:
                records = json.load(f)
            if records:
                sorted_records = sorted(records, key=lambda x: x.get("purchase_count", 0), reverse=True)[:N]
                return [(r["product_id"], r["purchase_count"]) for r in sorted_records]
    except Exception as e:
        logger.warning(f"Failed to generate popular products cache from Athena: {e}")
    # Fallback: ALS-based proxy
    logger.warning("Falling back to ALS-based proxy for popular products.")
    global spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()
    item_factors = als_model.itemFactors.toPandas()
    item_factors['norm'] = item_factors['features'].apply(lambda x: np.linalg.norm(x))
    top_items = item_factors.sort_values('norm', ascending=False).head(N)
    if normalize:
        top_items['normalized'] = top_items['norm'].apply(lambda x: round(consistent_normalize(x), 3))
        return list(zip(top_items['id'], top_items['normalized']))
    else:
        return list(zip(top_items['id'], top_items['norm']))

def get_similar_users(user_id, N=5):
    """
    Return the top-N most similar users to the given user_id using Spark ALSModel.
    """
    global spark, als_model
    if spark is None or als_model is None:
        initialize_ml_model()

    try:
        user_id_int = int(user_id)
    except Exception:
        return []

    user_factors = als_model.userFactors.toPandas()
    if user_factors.empty or user_id_int not in user_factors['id'].values:
        return []

    target_vec = np.array(user_factors[user_factors['id'] == user_id_int]['features'].values[0])
    from numpy import dot
    from numpy.linalg import norm
    similarities = []
    for idx, row in user_factors.iterrows():
        if row['id'] == user_id_int:
            continue
        sim = dot(target_vec, np.array(row['features'])) / (norm(target_vec) * norm(row['features']) + 1e-10)
        similarities.append((row['id'], sim))
    similarities.sort(key=lambda x: x[1], reverse=True)
    return [uid for uid, _ in similarities[:N]]
