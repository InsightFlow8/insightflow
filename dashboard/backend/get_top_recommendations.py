import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_top_n_recommendations(parquet_path: str = "user_seg.parquet/", n: int = 10) -> pd.DataFrame:
    """
    Get top N recommendations by popularity score for each cluster
    
    This function replicates the SQL query:
    SELECT cluster, product_id, pop_score, rank
    FROM recsys_curated.segment_popularity
    WHERE rank <= 10
    ORDER BY cluster, rank;
    
    Args:
        parquet_path (str): Path to the user segmentation parquet file
        n (int): Number of top recommendations to return per cluster
        
    Returns:
        pd.DataFrame: DataFrame with columns: cluster, product_id, pop_score, rank
    """
    try:
        # Load the parquet data
        logger.info(f"Loading data from {parquet_path}")
        df = pd.read_parquet(parquet_path)
        logger.info(f"Loaded data with shape: {df.shape}")
        
        # Sort by cluster and popularity score (descending)
        df_sorted = df.sort_values(['cluster', 'pop_score'], ascending=[True, False])
        
        # Add rank within each cluster
        df_sorted['rank'] = df_sorted.groupby('cluster')['pop_score'].rank(method='dense', ascending=False)
        
        # Filter to top N recommendations per cluster
        df_top_n = df_sorted[df_sorted['rank'] <= n]
        
        # Select and order columns to match the SQL query
        result = df_top_n[['cluster', 'product_id', 'pop_score', 'rank']].copy()
        
        # Final sort to match SQL ORDER BY cluster, rank
        result = result.sort_values(['cluster', 'rank'])
        
        logger.info(f"Retrieved top {n} recommendations for {result['cluster'].nunique()} clusters")
        return result
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise


def main():
    """Example usage of the function"""
    try:
        # Get top 10 recommendations (equivalent to your SQL query)
        recommendations = get_top_n_recommendations(n=10)
        
        print("=== Top 10 Recommendations by Cluster ===")
        print(recommendations)
        
        # You can also get different numbers of recommendations
        print("\n=== Top 5 Recommendations by Cluster ===")
        top_5 = get_top_n_recommendations(n=5)
        print(top_5)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
