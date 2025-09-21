from TableCreate import create_table
from File import File
import shutil

def main():

    table2 = "products"
    fields2 = [
        {"name": "product_id", "type": "i", "key": "primary", "index": "sequential"},
        {"name": "name", "type": "s", "length": 32},
        {"name": "price", "type": "f"},
        {"name": "stock", "type": "i"}
    ]

    create_table(table2, fields2)

    file = File(table2)


    products = [
        {"product_id": 1, "name": "Widget", "price": 9.99, "stock": 100},
        {"product_id": 2, "name": "Gadget", "price": 12.50, "stock": 50},
        {"product_id": 3, "name": "Tool", "price": 15.00, "stock": 30},
        {"product_id": 4, "name": "Device", "price": 8.75, "stock": 75},
        {"product_id": 5, "name": "Component", "price": 22.00, "stock": 25},
        {"product_id": 6, "name": "Part", "price": 18.99, "stock": 40},
        {"product_id": 7, "name": "Module", "price": 35.50, "stock": 15},
        {"product_id": 8, "name": "Element", "price": 11.25, "stock": 60},
        {"product_id": 9, "name": "Accessory", "price": 14.75, "stock": 45},
        {"product_id": 10, "name": "Instrument", "price": 28.00, "stock": 20},
        {"product_id": 11, "name": "Apparatus", "price": 16.99, "stock": 35},
        {"product_id": 12, "name": "Mechanism", "price": 42.50, "stock": 10},
        {"product_id": 13, "name": "System", "price": 19.25, "stock": 30},
        {"product_id": 14, "name": "Assembly", "price": 33.75, "stock": 18},
        {"product_id": 15, "name": "Unit", "price": 13.50, "stock": 55}
    ]

    for prod in products:
        params = {
            "op": "insert",
            "record": prod
        }
        file.execute(params)


    # Test single search
    search_params = {
        "op": "search",
        "field": "product_id",
        "value": 5
    }
    result = file.execute(search_params)
    print("Search result for product_id 5:", result)

    # Test range search by product_id (using sequential index)
    range_id_params = {
        "op": "range search",
        "field": "product_id", 
        "min": 3,
        "max": 10
    }
    result = file.execute(range_id_params)
    print("Range search result for product_id 3-10 BEFORE delete:", result, "\n")

    # Test delete using sequential index
    delete_params = {
        "op": "remove",
        "field": "product_id",
        "value": 7
    }
    result = file.execute(delete_params)

    result = file.execute(range_id_params)
    print("Range search result for product_id 3-10 AFTER delete:", result, "\n")

    # Test search for deleted item
    search_deleted = {
        "op": "search",
        "field": "product_id",
        "value": 7
    }
    result = file.execute(search_deleted)
    print("Search result for deleted product_id 7:", result)

    shutil.rmtree("files", ignore_errors=True)

if __name__ == "__main__":
    main()