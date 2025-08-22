"""Data: user_seg.parquet
Columns (example, actual data may vary):
user_id, cluster, r, f, m, r_z, f_z, m_z, r_cap

R (Recency): Take the days_since_prior_order from each user's last order.
Truncation: r_cap (default 60 days)
Direction: To achieve "closer = larger", take the negative sign before standardization (or use opposite direction after z-score).
F (Frequency): Total number of user orders, apply log1p smoothing.
M (Monetary proxy): avg(#items per order) (average number of items per order), then apply log1p smoothing.
#items per order comes from prior/train details (merge two directories to calculate the number of items per order).
Standardization: Apply z-score to R/F/M (mean/variance calculated from all users).
Clustering: KMeans(k=10) to get cluster (segment_id), using Spark ML built-in implementation.

Data: segment_popularity.parquet
Columns (example, actual data may vary):
cluster, product_id, pop_score, rank, support_seg, support_global"""

import pandas as pd
import logging
from typing import Optional, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserSegmentationAnalyzer:
    """
    Analyzer for user segmentation data to provide product recommendations
    """
    
    def __init__(self, user_seg_path: str = "/app/backend/user_seg.parquet/", 
                 segment_popularity_path: str = "/app/backend/segment_popularity.parquet/"):
        """
        Initialize the analyzer with paths to both parquet files
        
        Args:
            user_seg_path (str): Path to the user segmentation parquet file
            segment_popularity_path (str): Path to the segment popularity parquet file
        """
        self.user_seg_path = user_seg_path
        self.segment_popularity_path = segment_popularity_path
        self.user_seg_data = None
        self.segment_popularity_data = None
        self._load_data()
    
    def _load_data(self):
        """Load both parquet files into memory"""
        try:
            # Try multiple possible paths for the files
            user_seg_paths = [
                self.user_seg_path,
                "user_seg.parquet/",
                "./user_seg.parquet/",
                "/app/backend/user_seg.parquet/"
            ]
            
            segment_popularity_paths = [
                self.segment_popularity_path,
                "segment_popularity.parquet/",
                "./segment_popularity.parquet/",
                "/app/backend/segment_popularity.parquet/"
            ]
            
            # Try to load user segmentation data
            user_seg_loaded = False
            for path in user_seg_paths:
                try:
                    logger.info(f"Trying to load user segmentation data from {path}")
                    self.user_seg_data = pd.read_parquet(path)
                    
                    # Convert user_id and segment to proper integers
                    if 'user_id' in self.user_seg_data.columns:
                        self.user_seg_data['user_id'] = self.user_seg_data['user_id'].astype(int)
                    if 'segment' in self.user_seg_data.columns:
                        self.user_seg_data['segment'] = self.user_seg_data['segment'].astype(int)
                    
                    logger.info(f"User segmentation data loaded successfully from {path} with shape: {self.user_seg_data.shape}")
                    logger.info(f"User segmentation columns: {self.user_seg_data.columns.tolist()}")
                    logger.info(f"User segmentation data types: {self.user_seg_data.dtypes}")
                    user_seg_loaded = True
                    break
                except Exception as e:
                    logger.debug(f"Failed to load from {path}: {e}")
                    continue
            
            if not user_seg_loaded:
                raise FileNotFoundError("Could not load user segmentation data from any of the attempted paths")
            
            # Try to load segment popularity data
            segment_popularity_loaded = False
            for path in segment_popularity_paths:
                try:
                    logger.info(f"Trying to load segment popularity data from {path}")
                    self.segment_popularity_data = pd.read_parquet(path)
                    
                    # Convert segment and product_id to proper integers
                    if 'segment' in self.segment_popularity_data.columns:
                        self.segment_popularity_data['segment'] = self.segment_popularity_data['segment'].astype(int)
                    if 'product_id' in self.segment_popularity_data.columns:
                        self.segment_popularity_data['product_id'] = self.segment_popularity_data['product_id'].astype(int)
                    if 'rank' in self.segment_popularity_data.columns:
                        self.segment_popularity_data['rank'] = self.segment_popularity_data['rank'].astype(int)
                    
                    logger.info(f"Segment popularity data loaded successfully from {path} with shape: {self.segment_popularity_data.shape}")
                    logger.info(f"Segment popularity columns: {self.segment_popularity_data.columns.tolist()}")
                    logger.info(f"Segment popularity data types: {self.segment_popularity_data.dtypes}")
                    segment_popularity_loaded = True
                    break
                except Exception as e:
                    logger.debug(f"Failed to load from {path}: {e}")
                    continue
            
            if not segment_popularity_loaded:
                raise FileNotFoundError("Could not load segment popularity data from any of the attempted paths")
            
            # Validate and ensure consistent data types
            self._validate_data_types()
            
        except Exception as e:
            logger.error(f"Error loading parquet files: {e}")
            raise
    
    def _validate_data_types(self):
        """
        Validate and ensure consistent data types for the loaded data
        """
        try:
            if self.user_seg_data is not None:
                # Ensure user_id and segment are integers
                if 'user_id' in self.user_seg_data.columns:
                    self.user_seg_data['user_id'] = pd.to_numeric(self.user_seg_data['user_id'], errors='coerce').fillna(0).astype(int)
                if 'segment' in self.user_seg_data.columns:
                    self.user_seg_data['segment'] = pd.to_numeric(self.user_seg_data['segment'], errors='coerce').fillna(0).astype(int)
                
                logger.info(f"User segmentation data types after validation: {self.user_seg_data.dtypes}")
            
            if self.segment_popularity_data is not None:
                # Ensure segment, product_id, and rank are integers
                if 'segment' in self.segment_popularity_data.columns:
                    self.segment_popularity_data['segment'] = pd.to_numeric(self.segment_popularity_data['segment'], errors='coerce').fillna(0).astype(int)
                if 'product_id' in self.segment_popularity_data.columns:
                    self.segment_popularity_data['product_id'] = pd.to_numeric(self.segment_popularity_data['product_id'], errors='coerce').fillna(0).astype(int)
                if 'rank' in self.segment_popularity_data.columns:
                    self.segment_popularity_data['rank'] = pd.to_numeric(self.segment_popularity_data['rank'], errors='coerce').fillna(0).astype(int)
                
                logger.info(f"Segment popularity data types after validation: {self.segment_popularity_data.dtypes}")
                
        except Exception as e:
            logger.warning(f"Data type validation warning: {e}")
            # Continue with the data as-is if validation fails
    
    def is_data_available(self) -> bool:
        """
        Check if both data sources are available and loaded
        
        Returns:
            bool: True if both data sources are available, False otherwise
        """
        return (self.user_seg_data is not None and 
                self.segment_popularity_data is not None and
                not self.user_seg_data.empty and 
                not self.segment_popularity_data.empty)
    
    def get_data_status(self) -> dict:
        """
        Get the status of data loading
        
        Returns:
            dict: Status information about data availability
        """
        return {
            "user_seg_available": self.user_seg_data is not None and not self.user_seg_data.empty,
            "segment_popularity_available": self.segment_popularity_data is not None and not self.segment_popularity_data.empty,
            "user_seg_shape": self.user_seg_data.shape if self.user_seg_data is not None else None,
            "segment_popularity_shape": self.segment_popularity_data.shape if self.segment_popularity_data is not None else None,
            "user_seg_path": self.user_seg_path,
            "segment_popularity_path": self.segment_popularity_path
        }
    
    def get_sample_data(self, n: int = 3) -> dict:
        """
        Get sample data for debugging and verification
        
        Args:
            n (int): Number of sample rows to return
            
        Returns:
            dict: Sample data from both datasets
        """
        samples = {}
        
        if self.user_seg_data is not None and not self.user_seg_data.empty:
            samples['user_seg_sample'] = {
                'data': self.user_seg_data.head(n).to_dict('records'),
                'dtypes': self.user_seg_data.dtypes.to_dict()
            }
        
        if self.segment_popularity_data is not None and not self.segment_popularity_data.empty:
            samples['segment_popularity_sample'] = {
                'data': self.segment_popularity_data.head(n).to_dict('records'),
                'dtypes': self.segment_popularity_data.dtypes.to_dict()
            }
        
        return samples
    
    def get_user_cluster(self, user_id: int) -> Optional[int]:
        """
        Get the cluster ID for a given user ID
        
        Args:
            user_id (int): The user ID to look up
            
        Returns:
            int: The cluster ID (segment) for the user, or None if user not found
        """
        if self.user_seg_data is None:
            raise ValueError("User segmentation data not loaded. Please check the parquet file path.")
        
        try:
            # Find the user in the data
            user_data = self.user_seg_data[self.user_seg_data['user_id'] == user_id]
            
            if user_data.empty:
                logger.warning(f"User ID {user_id} not found in the data")
                return None
            
            # Get the cluster ID (segment)
            cluster_id = user_data.iloc[0]['segment']
            logger.info(f"User {user_id} belongs to cluster {cluster_id}")
            return int(cluster_id)
            
        except Exception as e:
            logger.error(f"Error getting cluster for user {user_id}: {e}")
            raise
    
    def get_top_n_recommendations_for_user(self, user_id: int, n: int = 10) -> pd.DataFrame:
        """
        Get top N product recommendations for a specific user based on their cluster
        
        Args:
            user_id (int): The user ID to get recommendations for
            n (int): Number of top recommendations to return (default: 10)
            
        Returns:
            pd.DataFrame: DataFrame with columns: segment, product_id, score, rank
        """
        if self.segment_popularity_data is None:
            raise ValueError("Segment popularity data not loaded. Please check the parquet file path.")
        
        try:
            # First get the user's cluster
            cluster_id = self.get_user_cluster(user_id)
            
            if cluster_id is None:
                logger.warning(f"Cannot get recommendations for user {user_id} - user not found")
                return pd.DataFrame(columns=['segment', 'product_id', 'score', 'rank'])
            
            # Get recommendations for that cluster
            cluster_recommendations = self.segment_popularity_data[
                self.segment_popularity_data['segment'] == cluster_id
            ].copy()
            
            # Sort by rank and get top N
            top_n_recommendations = cluster_recommendations.sort_values('rank').head(n)
            
            logger.info(f"Retrieved top {len(top_n_recommendations)} recommendations for user {user_id} (cluster {cluster_id})")
            return top_n_recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations for user {user_id}: {e}")
            raise
    
    def get_top_n_recommendations_by_cluster(self, cluster_id: int, n: int = 10) -> pd.DataFrame:
        """
        Get top N product recommendations for a specific cluster
        
        Args:
            cluster_id (int): The cluster ID to get recommendations for
            n (int): Number of top recommendations to return (default: 10)
            
        Returns:
            pd.DataFrame: DataFrame with columns: segment, product_id, score, rank
        """
        if self.segment_popularity_data is None:
            raise ValueError("Segment popularity data not loaded. Please check the parquet file path.")
        
        try:
            # Filter by cluster and get top N
            cluster_recommendations = self.segment_popularity_data[
                self.segment_popularity_data['segment'] == cluster_id
            ].copy()
            
            if cluster_recommendations.empty:
                logger.warning(f"No recommendations found for cluster {cluster_id}")
                return pd.DataFrame(columns=['segment', 'product_id', 'score', 'rank'])
            
            # Sort by rank and get top N
            top_n_recommendations = cluster_recommendations.sort_values('rank').head(n)
            
            logger.info(f"Retrieved top {len(top_n_recommendations)} recommendations for cluster {cluster_id}")
            return top_n_recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations for cluster {cluster_id}: {e}")
            raise
    
    def get_cluster_summary(self) -> pd.DataFrame:
        """
        Get a summary of clusters and their user counts
        
        Returns:
            pd.DataFrame: Summary statistics for each cluster
        """
        if self.user_seg_data is None:
            raise ValueError("User segmentation data not loaded. Please check the parquet file path.")
        
        summary = self.user_seg_data.groupby('segment').agg({
            'user_id': 'count',
            'R': ['mean', 'min', 'max'],
            'F': ['mean', 'min', 'max'],
            'M': ['mean', 'min', 'max']
        }).round(4)
        
        # Flatten column names
        summary.columns = ['_'.join(col).strip() for col in summary.columns]
        return summary.reset_index()
    
    def get_user_info(self, user_id: int) -> Optional[pd.Series]:
        """
        Get complete information for a specific user
        
        Args:
            user_id (int): The user ID to look up
            
        Returns:
            pd.Series: User information including cluster, R, F, M values, or None if not found
        """
        if self.user_seg_data is None:
            raise ValueError("User segmentation data not loaded. Please check the parquet file path.")
        
        try:
            user_data = self.user_seg_data[self.user_seg_data['user_id'] == user_id]
            
            if user_data.empty:
                logger.warning(f"User ID {user_id} not found in the data")
                return None
            
            return user_data.iloc[0]
            
        except Exception as e:
            logger.error(f"Error getting user info for user {user_id}: {e}")
            raise


def main():
    """Main function to demonstrate usage"""
    try:
        # Initialize the analyzer
        analyzer = UserSegmentationAnalyzer()
        
        # Example: Get cluster for a specific user
        test_user_id = 7  # Using the first user from the sample data
        cluster_id = analyzer.get_user_cluster(test_user_id)
        print(f"User {test_user_id} belongs to cluster {cluster_id}")
        
        # Get user information
        user_info = analyzer.get_user_info(test_user_id)
        if user_info is not None:
            print(f"\nUser {test_user_id} information:")
            print(f"Cluster: {user_info['segment']}")
            print(f"R (Recency): {user_info['R']}")
            print(f"F (Frequency): {user_info['F']}")
            print(f"M (Monetary): {user_info['M']}")
        
        # Get top 10 recommendations for the user
        print(f"\nTop 10 recommendations for user {test_user_id}:")
        recommendations = analyzer.get_top_n_recommendations_for_user(test_user_id, n=10)
        print(recommendations)
        
        # Get cluster summary
        print("\n=== Cluster Summary ===")
        summary = analyzer.get_cluster_summary()
        print(summary)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

