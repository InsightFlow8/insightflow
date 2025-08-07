#!/usr/bin/env python3
"""
Test script for fixed Athena queries with proper type casting.
"""

from analysis_athena import AthenaAnalyzer, test_data_types, test_connection

def test_fixed_queries():
    """Test the fixed queries with proper type casting"""
    print("🔧 Testing Fixed Athena Queries")
    print("=" * 50)
    
    try:
        # Initialize analyzer
        athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
        print("✅ Athena Analyzer initialized")
        
        # Clear cache
        print("🗑️ Clearing cache...")
        athena_analyzer.clear_cache()
        
        # Test 1: Customer Journey Analysis
        print("\n👥 Testing Customer Journey Analysis (Fixed)...")
        try:
            fig, df = athena_analyzer.create_customer_journey_analysis(use_cache=False)
            print(f"✅ Customer Journey Analysis completed - {len(df)} rows")
            print("📊 Sample data:")
            print(df.head())
        except Exception as e:
            print(f"❌ Customer Journey Analysis failed: {e}")
        
        # Test 2: Lifetime Value Analysis
        print("\n💰 Testing Lifetime Value Analysis (Fixed)...")
        try:
            fig, df = athena_analyzer.create_lifetime_value_analysis(use_cache=False)
            print(f"✅ Lifetime Value Analysis completed - {len(df)} rows")
            print("📊 Sample data:")
            print(df.head())
        except Exception as e:
            print(f"❌ Lifetime Value Analysis failed: {e}")
        
        # Test 3: Product Affinity Analysis
        print("\n🔗 Testing Product Affinity Analysis (Fixed)...")
        try:
            fig, df = athena_analyzer.create_product_affinity_analysis(top_products=10, use_cache=False)
            print(f"✅ Product Affinity Analysis completed - {len(df)} rows")
            print("📊 Sample data:")
            print(df.head())
        except Exception as e:
            print(f"❌ Product Affinity Analysis failed: {e}")
        
        # Test 4: Churn Analysis
        print("\n📉 Testing Churn Analysis (Fixed)...")
        try:
            fig, df = athena_analyzer.create_churn_analysis(use_cache=False)
            print(f"✅ Churn Analysis completed - {len(df)} rows")
            print("📊 Sample data:")
            print(df.head())
        except Exception as e:
            print(f"❌ Churn Analysis failed: {e}")
        
        print("\n🎉 All fixed query tests completed!")
        
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")

if __name__ == "__main__":
    # First test connection
    if test_connection():
        # Then test data types
        test_data_types()
        # Finally test fixed queries
        test_fixed_queries()
    else:
        print("❌ Connection test failed, skipping other tests") 