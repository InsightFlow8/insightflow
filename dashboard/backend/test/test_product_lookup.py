from product_lookup import get_product_by_id, get_all_products, search_products

def test_product_lookup():
    for id in [45000, 45006, 778, 2358]:
        print(f"Testing get_product_by_id('{id}'):")
        product = get_product_by_id(str(id))
        print(product)

    all_products = get_all_products()
    print("Total products:", len(all_products))
    print("'45000' in all_products:", "45000" in all_products)
    print("'45006' in all_products:", "45006" in all_products)

    print("\nTesting search_products('Pinot Gris'):")
    results = search_products("Pinot Gris")
    for r in results:
        print(r)

if __name__ == "__main__":
    test_product_lookup()