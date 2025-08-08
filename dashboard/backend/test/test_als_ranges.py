#!/usr/bin/env python3
"""
Test script to analyze ALS score ranges
This will help us understand what the actual ALS prediction scores look like
"""

import logging
import sys
import os

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ml_model import initialize_ml_model, test_als_score_ranges, analyze_als_score_ranges

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

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