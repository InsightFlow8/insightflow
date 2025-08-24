#!/usr/bin/env python3
"""
Test script to verify user segmentation analyzer functionality
"""
import os
import sys
import logging

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(__file__))

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_user_seg_analyzer():
    """Test the user segmentation analyzer directly"""
    try:
        from user_segmentation_analysis import UserSegmentationAnalyzer
        
        logger.info("Testing user segmentation analyzer...")
        
        # Initialize the analyzer
        analyzer = UserSegmentationAnalyzer()
        
        # Check if data is available
        if analyzer.is_data_available():
            logger.info("✅ User segmentation analyzer data is available")
            
            # Get data status
            status = analyzer.get_data_status()
            logger.info(f"Data status: {status}")
            
            # Test with a sample user
            test_user_id = 555  # From the logs
            try:
                cluster = analyzer.get_user_cluster(test_user_id)
                if cluster is not None:
                    logger.info(f"✅ User {test_user_id} belongs to cluster {cluster}")
                    
                    # Get user info
                    user_info = analyzer.get_user_info(test_user_id)
                    if user_info is not None:
                        logger.info(f"User {test_user_id} info:")
                        logger.info(f"  Cluster: {user_info.get('segment', 'N/A')}")
                        logger.info(f"  R (Recency): {user_info.get('R', 'N/A'):.3f}")
                        logger.info(f"  F (Frequency): {user_info.get('F', 'N/A'):.3f}")
                        logger.info(f"  M (Monetary): {user_info.get('M', 'N/A'):.3f}")
                    
                    # Get similar users
                    similar_users = analyzer.user_seg_data[
                        analyzer.user_seg_data['segment'] == cluster
                    ]['user_id'].tolist()
                    
                    # Filter out the original user
                    similar_users = [u for u in similar_users if u != test_user_id]
                    logger.info(f"Found {len(similar_users)} similar users in cluster {cluster}")
                    
                    if similar_users:
                        logger.info(f"Sample similar users: {similar_users[:5]}")
                    
                else:
                    logger.warning(f"⚠️ User {test_user_id} not found in segmentation data")
                    
            except Exception as e:
                logger.error(f"❌ Error testing user {test_user_id}: {e}")
                
        else:
            logger.error("❌ User segmentation analyzer data is not available")
            
            # Try to get more details about what's wrong
            try:
                status = analyzer.get_data_status()
                logger.info(f"Data status: {status}")
                
                # Try to get sample data
                samples = analyzer.get_sample_data()
                logger.info(f"Sample data: {samples}")
                
            except Exception as e:
                logger.error(f"❌ Could not get data status: {e}")
                
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

def test_cache_initialization():
    """Test cache initialization"""
    try:
        from base_tools import ensure_user_seg_analyzer_sync, get_cache_status_sync, _cache
        
        logger.info("Testing cache initialization...")
        
        # Check cache status before
        status_before = get_cache_status_sync()
        logger.info(f"Cache status before: {status_before}")
        
        # Check raw cache
        logger.info(f"Raw cache keys: {list(_cache.keys())}")
        logger.info(f"Raw cache content: {_cache}")
        
        # Try to get analyzer
        try:
            analyzer = ensure_user_seg_analyzer_sync()
            
            if analyzer:
                logger.info("✅ User segmentation analyzer retrieved from cache")
                
                # Check cache status after
                status_after = get_cache_status_sync()
                logger.info(f"Cache status after: {status_after}")
                
            else:
                logger.error("❌ Failed to get user segmentation analyzer from cache")
                
        except Exception as e:
            logger.error(f"❌ Exception in ensure_user_seg_analyzer_sync: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
    except Exception as e:
        logger.error(f"❌ Cache test failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    logger.info("Starting user segmentation analyzer tests...")
    
    # Test 1: Direct analyzer initialization
    test_user_seg_analyzer()
    
    print("\n" + "="*50 + "\n")
    
    # Test 2: Cache initialization
    test_cache_initialization()
    
    logger.info("Tests completed.")
