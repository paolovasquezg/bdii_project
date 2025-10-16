from backend.catalog.ddl import create_table
from backend.storage.file import File
import shutil


def HeapTest():
    print("\n=== Testing Heap (No Index) ===")
    table = "heap_products"
    fields = [
        {"name": "product_id", "type": "i", "key": "indexes"},
        {"name": "name", "type": "s", "length": 32},
        {"name": "price", "type": "f"},
        {"name": "stock", "type": "i"}
    ]
    create_table(table, fields)
    file = File(table)
    products = [
        {"product_id": 1, "name": "Widget", "price": 9.99, "stock": 100},
        {"product_id": 2, "name": "Gadget", "price": 12.50, "stock": 50},
        {"product_id": 3, "name": "Tool", "price": 15.00, "stock": 30},
        {"product_id": 4, "name": "Device", "price": 8.75, "stock": 75}
    ]
    for prod in products:
        params = {
            "op": "insert",
            "record": prod
        }
        file.execute(params)
    # Test search by product_id
    search_params = {
        "op": "search",
        "field": "product_id",
        "value": 3
    }
    result = file.execute(search_params)
    print("Heap search result for product_id 3:", result)
    # Test delete by name
    delete_params = {
        "op": "remove",
        "field": "name",
        "value": "Device"
    }
    delete_result = file.execute(delete_params)
    print("Heap delete result for name 'Device':", delete_result)
    # Test range search
    range_params = {
        "op": "range search",
        "field": "product_id",
        "min": 1,
        "max": 4
    }
    range_result = file.execute(range_params)
    print("Heap range search result for product_id 1-4:", range_result)
    shutil.rmtree("../runtime/files", ignore_errors=True)


def SeqTest():

    table2 = "products"
    fields2 = [
        {"name": "product_id", "type": "i", "key": "indexes", "index": "sequential"},
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

    # Test delete using non-indexes key field (name)
    print("Testing delete by name...")
    delete_params = {
        "op": "remove",
        "field": "name",
        "value": "Module"
    }
    result = file.execute(delete_params)
    print("Delete by name result:", result)

    result = file.execute(range_id_params)
    print("Range search result for product_id 3-10 AFTER delete by name:", result, "\n")

    # Test search by name (non-indexes key)
    search_by_name = {
        "op": "search",
        "field": "name",
        "value": "Component"
    }
    result = file.execute(search_by_name)
    print("Search result for name 'Component':", result)

    # Test search by price (non-indexes key)
    search_by_price = {
        "op": "search",
        "field": "price",
        "value": 22.0
    }
    result = file.execute(search_by_price)
    print("Search result for price 22.0:", result)

    # Test delete by price (non-indexes key)
    delete_by_price = {
        "op": "remove",
        "field": "price", 
        "value": 35.50
    }
    result = file.execute(delete_by_price)
    print("Delete by price result:", result)

    shutil.rmtree("../runtime/files", ignore_errors=True)


def IsamTest():
    table2 = "products"
    fields2 = [
        {"name": "product_id", "type": "i", "key": "indexes", "index": "isam"},
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
        {"product_id": 15, "name": "Unit", "price": 13.50, "stock": 55},
        {"product_id": 16, "name": "Controller", "price": 45.00, "stock": 12},
        {"product_id": 17, "name": "Sensor", "price": 23.75, "stock": 65},
        {"product_id": 18, "name": "Actuator", "price": 38.50, "stock": 28},
        {"product_id": 19, "name": "Processor", "price": 125.00, "stock": 8},
        {"product_id": 20, "name": "Memory", "price": 67.25, "stock": 15},
        {"product_id": 21, "name": "Connector", "price": 5.99, "stock": 200},
        {"product_id": 22, "name": "Cable", "price": 8.50, "stock": 150},
        {"product_id": 23, "name": "Switch", "price": 12.75, "stock": 80},
        {"product_id": 24, "name": "Relay", "price": 15.50, "stock": 45},
        {"product_id": 25, "name": "Capacitor", "price": 3.25, "stock": 300},
        {"product_id": 26, "name": "Resistor", "price": 1.50, "stock": 500},
        {"product_id": 27, "name": "Transistor", "price": 4.75, "stock": 250},
        {"product_id": 28, "name": "Diode", "price": 2.25, "stock": 400},
        {"product_id": 29, "name": "LED", "price": 1.99, "stock": 350},
        {"product_id": 30, "name": "Battery", "price": 25.50, "stock": 60},
        {"product_id": 31, "name": "Microcontroller", "price": 15.75, "stock": 120},
        {"product_id": 32, "name": "Arduino", "price": 35.00, "stock": 80},
        {"product_id": 33, "name": "Raspberry Pi", "price": 75.00, "stock": 45},
        {"product_id": 34, "name": "Breadboard", "price": 8.99, "stock": 200},
        {"product_id": 35, "name": "Jumper Wires", "price": 5.50, "stock": 300},
        {"product_id": 36, "name": "Power Supply", "price": 45.99, "stock": 60},
        {"product_id": 37, "name": "Oscilloscope", "price": 299.99, "stock": 15},
        {"product_id": 38, "name": "Multimeter", "price": 89.50, "stock": 35},
        {"product_id": 39, "name": "Soldering Iron", "price": 29.99, "stock": 70},
        {"product_id": 40, "name": "Heat Sink", "price": 3.75, "stock": 250},
        {"product_id": 41, "name": "Fan", "price": 12.25, "stock": 150},
        {"product_id": 42, "name": "LCD Display", "price": 22.50, "stock": 90},
        {"product_id": 43, "name": "OLED Display", "price": 18.99, "stock": 110},
        {"product_id": 44, "name": "Servo Motor", "price": 14.75, "stock": 85},
        {"product_id": 45, "name": "Stepper Motor", "price": 28.00, "stock": 55},
        {"product_id": 46, "name": "DC Motor", "price": 9.99, "stock": 175},
        {"product_id": 47, "name": "Encoder", "price": 16.50, "stock": 65},
        {"product_id": 48, "name": "Gyroscope", "price": 24.99, "stock": 40},
        {"product_id": 49, "name": "Accelerometer", "price": 19.75, "stock": 75},
        {"product_id": 50, "name": "Temperature Sensor", "price": 7.25, "stock": 180}
    ]

    build_params = {
        "op": "build",
        "records": products
    }
    
    file.execute(build_params)
    

    print("\n=== Testing ISAM Delete (Short Test) ===")
    # Test delete by product_id
    delete_params_id = {
        "op": "remove",
        "field": "product_id",
        "value": 45
    }
    delete_result_id = file.execute(delete_params_id)
    print("Delete result for product_id 45:", delete_result_id)

    # Verify deletion by searching again
    search_deleted_id = {
        "op": "search",
        "field": "product_id",
        "value": 45
    }
    result_deleted_id = file.execute(search_deleted_id)
    print("Search result for deleted product_id 45:", result_deleted_id)

    print("\n=== Testing ISAM Delete (Multiple) and Range Search ===")
    # Delete several product_ids
    for pid in [45, 46, 47, 48, 49, 50]:
        delete_params = {
            "op": "remove",
            "field": "product_id",
            "value": pid
        }
        delete_result = file.execute(delete_params)
        print(f"Delete result for product_id {pid}:", delete_result)

    # Range search for deleted range
    range_deleted_params = {
        "op": "range search",
        "field": "product_id",
        "min": 45,
        "max": 50
    }
    result_range_deleted = file.execute(range_deleted_params)
    print("Range search result for product_id 45-50 after deletes:", result_range_deleted)

    print("\n=== Testing ISAM Delete by Another Field (name) ===")
    # Delete by name
    for name in ["LED", "Gyroscope", "Servo Motor"]:
        delete_params = {
            "op": "remove",
            "field": "name",
            "value": name
        }
        delete_result = file.execute(delete_params)
        print(f"Delete result for name '{name}':", delete_result)

    # Range search for product_id 28-44 to check deletions
    range_deleted_name_params = {
        "op": "range search",
        "field": "product_id",
        "min": 28,
        "max": 44
    }
    result_range_deleted_name = file.execute(range_deleted_name_params)
    print("Range search result for product_id 28-44 after name deletes:", result_range_deleted_name)

    shutil.rmtree("../runtime/files", ignore_errors=True)

if __name__ == "__main__":
    IsamTest()