"""
Product lookup system that works independently of S3Vectors metadata
"""

import os
import json
import logging
import pandas as pd
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ProductLookup:
    """
    Product lookup system that stores product information separately from S3Vectors
    since S3Vectors metadata is not working in the preview version
    """
    
    def __init__(self, cache_file: str = "product_lookup_cache.json"):
        self.cache_file = cache_file
        self.products = {}
        self.load_cache()
    
    def load_cache(self):
        """Load product lookup from cache file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self.products = json.load(f)
                logger.info(f"✅ Loaded {len(self.products)} products from cache")
            else:
                logger.info("📝 No cache file found, will build from data")
                self.products = {}
        except Exception as e:
            logger.error(f"❌ Error loading cache: {e}")
            self.products = {}
    
    def save_cache(self):
        """Save product lookup to cache file"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.products, f, indent=2)
            logger.info(f"✅ Saved {len(self.products)} products to cache")
        except Exception as e:
            logger.error(f"❌ Error saving cache: {e}")
    
    def build_from_products_data(self, products_data: pd.DataFrame):
        """Build product lookup from products DataFrame"""
        try:
            logger.info(f"🔄 Building product lookup from {len(products_data)} products...")
            
            self.products = {}
            for _, row in products_data.iterrows():
                product_id = str(row['product_id'])
                
                # Create product metadata
                product_info = {
                    'product_id': product_id,
                    'product_name': row['product_name'],
                    'aisle': row.get('aisle', ''),
                    'department': row.get('department', ''),
                    'aisle_id': str(row['aisle_id']),
                    'department_id': str(row['department_id']),
                    'content': f"product name：{row['product_name']}；aisle：{row.get('aisle', '')}；department：{row.get('department', '')}"
                }
                
                # Store by product_id
                self.products[product_id] = product_info
            
            logger.info(f"✅ Built product lookup with {len(products_data)} products")
            self.save_cache()
            
        except Exception as e:
            logger.error(f"❌ Error building product lookup: {e}")
    
    def get_product_by_id(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Get product information by product ID"""
        return self.products.get(str(product_id))
    
    def get_product_by_vector_key(self, vector_key: str) -> Optional[Dict[str, Any]]:
        """Get product information by vector key (e.g., 'product_123')"""
        return self.products.get(vector_key)
    
    def get_all_products(self) -> Dict[str, Dict[str, Any]]:
        """Get all products (by product_id, not vector_key)"""
        # Filter to only product_id entries (not vector_key entries)
        return {k: v for k, v in self.products.items() if not k.startswith('product_')}
    
    def search_products(self, query: str, limit: int = 10) -> list:
        """Search products by name, aisle, or department"""
        query = query.lower()
        results = []
        
        for product_id, product_info in self.products.items():
            if product_id.startswith('product_'):
                continue  # Skip vector_key entries
                
            # Search in product name, aisle, and department
            if (query in product_info.get('product_name', '').lower() or
                query in product_info.get('aisle', '').lower() or
                query in product_info.get('department', '').lower()):
                
                results.append({
                    'product_id': product_info['product_id'],
                    'product_name': product_info['product_name'],
                    'aisle': product_info.get('aisle', ''),
                    'department': product_info.get('department', ''),
                    'score': 1.0  # Simple matching for now
                })
                
                if len(results) >= limit:
                    break
        
        return results

# Global instance
_product_lookup = None

def get_product_lookup() -> ProductLookup:
    """Get the global product lookup instance"""
    global _product_lookup
    if _product_lookup is None:
        _product_lookup = ProductLookup()
    return _product_lookup

def build_product_lookup_from_data(products_data: pd.DataFrame):
    """Build the global product lookup from products data"""
    global _product_lookup
    _product_lookup = ProductLookup()
    _product_lookup.build_from_products_data(products_data)

def get_product_by_id(product_id: str) -> Optional[Dict[str, Any]]:
    """Get product information by product ID"""
    lookup = get_product_lookup()
    return lookup.get_product_by_id(product_id)

def get_product_by_vector_key(vector_key: str) -> Optional[Dict[str, Any]]:
    """Get product information by vector key"""
    lookup = get_product_lookup()
    return lookup.get_product_by_vector_key(vector_key)

def get_all_products() -> Dict[str, Dict[str, Any]]:
    """Get all products"""
    lookup = get_product_lookup()
    return lookup.get_all_products()

def search_products(query: str, limit: int = 10) -> list:
    """Search products"""
    lookup = get_product_lookup()
    return lookup.search_products(query, limit) 