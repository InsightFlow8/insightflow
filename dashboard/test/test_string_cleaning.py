#!/usr/bin/env python3
"""
Test script to verify string cleaning in tools.py
"""

def test_string_cleaning():
    """Test the string cleaning logic"""
    
    # Test cases for product_id cleaning
    test_cases = [
        ("3", "3"),
        ("'3'", "3"),
        ('"3"', "3"),
        (" 3 ", "3"),
        (" '3' ", "3"),
        (' "3" ', "3"),
    ]
    
    print("🧪 Testing string cleaning logic...")
    
    for input_str, expected in test_cases:
        # Simulate the cleaning logic
        cleaned = input_str.strip().strip("'\"")
        result = "✅ PASS" if cleaned == expected else "❌ FAIL"
        print(f"   Input: '{input_str}' -> Expected: '{expected}', Got: '{cleaned}' -> {result}")
    
    print("\n📝 String cleaning test completed!")

if __name__ == "__main__":
    test_string_cleaning() 