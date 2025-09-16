from Methods import load_tables, put_json, save_tables
import os
    
def create_table(table: str, fields: list):
    os.makedirs(f"files/{table}", exist_ok=True)
    tables = load_tables()

    if table in tables:
        return
    indexes = {}
    new_fields = {}
    new_schema = []
    for field in fields:
        if "index" in field:
            
            if field["index"] in ("sequential", "isam"):
                if "key" not in field or field["key"] != "primary":
                    return

            index = {"index": field["index"], "filename": f"files/{table}/{table}-{field["index"]}-{field["name"]}.dat"}
            if "key" in field and field["key"] == "primary":
                indexes["primary"] = index
            
            indexes[field["name"]] = index
        
        new_fields[field["name"]] = {"type": field["type"]}
        if "key" in field:
            new_fields[field["name"]]["key"] = field["key"]
        
        new_field = field.copy()

        if "index" in field:
            new_field.pop("index")

        if "key" in field:
            new_field.pop("key")

        new_schema.append(new_field)
            
                
    if "primary" not in indexes:
        indexes["primary"] = {"index": "heap", "filename": f"files/{table}/{table}-heap.dat"}

    table_file = f"files/{table}/{table}.dat"

    put_json(table_file, [new_fields, indexes])
    
    mainfilename = ""
    for index in indexes:
        
        put_schema = []

        if (index == "primary"):
            mainfilename = indexes[index]["filename"]
            put_schema = new_schema
            put_schema.append({"name": "deleted", "type": "?"})
            put_json(mainfilename, [put_schema])
        else:

            if (indexes[index]["filename"] == mainfilename):
                continue
            
            for schem in new_schema:
                if schem["name"] == index:
                    put_schema.append(schem)
                    put_schema.append({"name": "pos", "type": "i"})
                    put_schema.append({"name": "deleted", "type": "?"})
                    put_json(indexes[index]["filename"], [put_schema])
                continue
    
    tables[table] = table_file
    save_tables(tables)