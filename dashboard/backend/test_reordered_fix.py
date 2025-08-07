#!/usr/bin/env python3
"""
Test script to verify the reordered string boolean fix.
"""

from analysis_athena import AthenaAnalyzer

def test_reordered_fix():
    """Test the fixed reordered string boolean handling"""
    print("🔧 Testing Reordered String Boolean Fix")
    print("=" * 50)
    
    try:
        # Initialize analyzer
        athena_analyzer = AthenaAnalyzer(profile_name='insightflow')
        print("✅ Athena Analyzer initialized")
        
        # Test 1: Simple reordered query
        print("\n👥 Testing Customer Journey Analysis with string boolean...")
        try:
            fig, df = athena_analyzer.create_customer_journey_analysis(use_cache=False)
            print(f"✅ Customer Journey Analysis completed - {len(df)} rows")
            if len(df) > 0:
                print("📊 Sample data:")
                print(df.head())
            else:
                print("⚠️ No data returned")
        except Exception as e:
            print(f"❌ Customer Journey Analysis failed: {e}")
        
        # Test 2: Lifetime Value Analysis
        print("\n💰 Testing Lifetime Value Analysis with string boolean...")
        try:
            fig, df = athena_analyzer.create_lifetime_value_analysis(use_cache=False)
            print(f"✅ Lifetime Value Analysis completed - {len(df)} rows")
            if len(df) > 0:
                print("📊 Sample data:")
                print(df.head())
            else:
                print("⚠️ No data returned")
        except Exception as e:
            print(f"❌ Lifetime Value Analysis failed: {e}")
        
        print("\n🎉 String boolean fix tests completed!")
        
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")

if __name__ == "__main__":
    test_reordered_fix() 