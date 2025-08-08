import pickle
import os
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class AnalysisCache:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        try:
            os.makedirs(cache_dir, exist_ok=True)
            logger.info(f"📁 Cache directory: {cache_dir}")
        except Exception as e:
            logger.error(f"Failed to create cache directory: {e}")
            # Fallback to current directory
            self.cache_dir = "."
    
    def _get_cache_key(self, function_name, data_hash, params):
        """Generate cache key based on function and data"""
        try:
            param_str = str(sorted(params.items()))
            key_data = f"{function_name}_{data_hash}_{param_str}"
            return hashlib.md5(key_data.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Failed to generate cache key: {e}")
            return f"{function_name}_{data_hash}"
    
    def _get_data_hash(self, df):
        """Generate hash of dataframe for cache invalidation"""
        try:
            if df is None or df.empty:
                return "empty_data"
            
            # Use a more efficient hash for large dataframes
            sample_size = min(1000, len(df))
            sample_df = df.sample(n=sample_size, random_state=42)
            return hashlib.md5(pd.util.hash_pandas_object(sample_df).values).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to generate data hash: {e}")
            return "default_hash"
    
    def get_cached_result(self, function_name, df, params=None):
        """Get cached analysis result if available and fresh"""
        if params is None:
            params = {}
        
        try:
            data_hash = self._get_data_hash(df)
            cache_key = self._get_cache_key(function_name, data_hash, params)
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
            
            # Check if cache exists and is fresh (less than 1 minute old for testing)
            if os.path.exists(cache_file):
                file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
                if file_age < timedelta(hours=6): # minutes=1
                    try:
                        with open(cache_file, 'rb') as f:
                            result = pickle.load(f)
                        logger.info(f"✅ Using cached result for {function_name}")
                        return result
                    except Exception as e:
                        logger.warning(f"Failed to load cache for {function_name}: {e}")
                        # Remove corrupted cache file
                        try:
                            os.remove(cache_file)
                        except:
                            pass
                else:
                    logger.info(f"🔄 Cache expired for {function_name}")
                    # Remove expired cache file
                    try:
                        os.remove(cache_file)
                    except:
                        pass
            
            logger.info(f"🔄 No fresh cache found for {function_name}")
            return None
            
        except Exception as e:
            logger.error(f"Cache lookup error for {function_name}: {e}")
            return None
    
    def save_result(self, function_name, df, result, params=None):
        """Save analysis result to cache"""
        if params is None:
            params = {}
        
        try:
            data_hash = self._get_data_hash(df)
            cache_key = self._get_cache_key(function_name, data_hash, params)
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
            
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            
            logger.info(f"💾 Cached result for {function_name}")
            
        except Exception as e:
            logger.error(f"Failed to save cache for {function_name}: {e}")

# Global cache instance
analysis_cache = AnalysisCache()