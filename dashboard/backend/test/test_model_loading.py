#!/usr/bin/env python3
"""
Test script to verify ALS model loading in EC2 environment
"""

import os
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_model_loading():
    """Test the model loading functionality"""
    print("🧪 Testing ALS Model Loading")
    print("=" * 40)
    
    try:
        # Import the ML model module
        from ml_model import initialize_ml_model, verify_model_file
        
        # Check environment
        model_path = os.getenv("ALS_MODEL_PATH", "als_model.pkl")
        print(f"📁 Model path: {model_path}")
        print(f"📁 Current directory: {os.getcwd()}")
        
        # Check if model file exists
        if os.path.exists(model_path):
            file_size = os.path.getsize(model_path)
            print(f"✅ Model file exists: {model_path} ({file_size} bytes)")
            
            # Verify model file
            if verify_model_file(model_path):
                print("✅ Model file verification passed")
            else:
                print("❌ Model file verification failed")
                return 1
        else:
            print(f"❌ Model file not found: {model_path}")
            print("💡 Ensure the model is downloaded from S3")
            return 1
        
        # Test model initialization
        print("\n🚀 Testing model initialization...")
        initialize_ml_model()
        
        print("\n✅ Model loading test completed successfully!")
        return 0
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

def main():
    """Main function"""
    return test_model_loading()

if __name__ == "__main__":
    exit(main()) 