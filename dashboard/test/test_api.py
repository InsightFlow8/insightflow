#!/usr/bin/env python3
"""
Test script to check if the /data API is working properly
"""

import requests
import json
import sys

def test_data_api():
    """Test the /data API endpoint"""
    
    # API URL
    api_url = "http://localhost:8000/data"
    
    print("🔍 Testing /data API...")
    print(f"URL: {api_url}")
    
    try:
        # Make GET request to /data endpoint
        print("📡 Sending GET request...")
        response = requests.get(api_url, timeout=600)
        
        print(f"📊 Response Status: {response.status_code}")
        print(f"📊 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ API is working!")
            
            # Try to parse JSON response
            try:
                data = response.json()
                print(f"📦 Data keys: {list(data.keys())}")
                
                # Check each DataFrame
                for key, records in data.items():
                    print(f"  - {key}: {len(records)} records")
                    if records:
                        print(f"    Sample columns: {list(records[0].keys())}")
                
                print("✅ JSON parsing successful!")
                return True
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing failed: {e}")
                print(f"Response content (first 500 chars): {response.text[:500]}")
                return False
                
        else:
            print(f"❌ API returned error status: {response.status_code}")
            print(f"Error response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed - Is the backend running?")
        print("Make sure to run: docker-compose up backend")
        return False
        
    except requests.exceptions.Timeout:
        print("❌ Request timed out - Backend might be slow")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_health_api():
    """Test the /health API endpoint"""
    
    api_url = "http://localhost:8000/health"
    
    print("\n🔍 Testing /health API...")
    
    try:
        response = requests.get(api_url, timeout=10)
        print(f"📊 Health Status: {response.status_code}")
        
        if response.status_code == 200:
            health_data = response.json()
            print(f"📊 Health Data: {health_data}")
            return True
        else:
            print(f"❌ Health check failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 API Testing Tool")
    print("=" * 50)
    
    # Test health first
    health_ok = test_health_api()
    
    # Test data API
    data_ok = test_data_api()
    
    print("\n" + "=" * 50)
    if health_ok and data_ok:
        print("🎉 All APIs are working properly!")
    else:
        print("❌ Some APIs have issues. Check the logs above.") 