from TableCreate import create_table
from File import File
import shutil

def main():

    table2 = "products"
    fields2 = [
        {"name": "product_id", "type": "i"},
        {"name": "name", "type": "s", "length": 32},
        {"name": "price", "type": "f"},
        {"name": "stock", "type": "i"}
    ]

    create_table(table2, fields2)

    file = File(table2)

    products = [
        {"product_id": 1, "name": "Widget", "price": 9.99, "stock": 100},
        {"product_id": 2, "name": "Widget", "price": 12.50, "stock": 50},
        {"product_id": 3, "name": "Widget", "price": 15.00, "stock": 30}
    ]

    for prod in products:
        params = {
            "op": "insert",
            "record": prod
        }
        file.execute(params)


    range_params = {
        "op": "range search",
        "field": "price",
        "min": 10,
        "max": 15
    }
    result = file.execute(range_params)
    print("Range search result BEFORE delete:", result)

    remove_params = {
        "op": "remove",
        "field": "product_id",
        "value": 2
    }
    file.execute(remove_params)

    result = file.execute(range_params)
    print("Range search result AFTER delete:", result)

    shutil.rmtree("files", ignore_errors=True)

if __name__ == "__main__":
    main()