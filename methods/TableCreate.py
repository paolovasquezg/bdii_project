from methods.Methods import load_tables, put_json, save_tables
import os

def create_table(table: str, fields: list):
    os.makedirs(f"files/{table}", exist_ok=True)
    tables = load_tables()

    if table in tables:
        return

    indexes = {}
    new_fields = {}
    new_schema = []
    pk_name = None  # nombre de la primary key
    rtree_fields = set()  # << NUEVO: registrar columnas con índice rtree

    for field in fields:
        # ------ definición de índices ------
        if "index" in field:

            # sequential / isam solo si son primary
            if field["index"] in ("sequential", "isam"):
                if "key" not in field or field["key"] != "primary":
                    return

            # ruta del archivo de índice
            if field["index"] == "rtree":
                # índice espacial: archivo binario propio
                idx_dir = f"files/{table}/idx/{field['name']}"
                os.makedirs(idx_dir, exist_ok=True)
                idx_filename = f"{idx_dir}/rtree.idx"
                index = {"index": field["index"], "filename": idx_filename}
                rtree_fields.add(field["name"])  # << NUEVO
            else:
                # resto de índices mantienen el formato original
                idx_filename = f"files/{table}/{table}-{field['index']}-{field['name']}.dat"
                index = {"index": field["index"], "filename": idx_filename}

            # registrar índice (y primary si aplica)
            if "key" in field and field["key"] == "primary":
                indexes["primary"] = index
                pk_name = field["name"]  # guardamos el nombre de la PK

            indexes[field["name"]] = index

        # ------ metadatos del campo ------
        new_fields[field["name"]] = {"type": field["type"]}
        if "key" in field:
            new_fields[field["name"]]["key"] = field["key"]
        if "length" in field:
            new_fields[field["name"]]["length"] = field["length"]

        # schema “limpio” para escribir en la tabla principal
        new_field = field.copy()
        if "index" in new_field:
            new_field.pop("index")
        if "key" in new_field:
            new_field.pop("key")
        new_schema.append(new_field)

    # Si no hay índice primary declarado, la tabla es heap
    if "primary" not in indexes:
        indexes["primary"] = {"index": "heap", "filename": f"files/{table}/{table}-heap.dat"}

    # Catálogo de la tabla
    table_file = f"files/{table}/{table}.dat"
    put_json(table_file, [new_fields, indexes])

    # Crear archivos por índice
    mainfilename = ""
    for index in indexes:
        put_schema = []

        if index == "primary":
            mainfilename = indexes[index]["filename"]

            # << NUEVO: excluir del schema del archivo primario los campos rtree
            filtered_schema = [s for s in new_schema if s["name"] not in rtree_fields]
            put_schema = list(filtered_schema)
            put_schema.append({"name": "deleted", "type": "?"})
            put_json(mainfilename, [put_schema])

        else:
            # Evitar escribir dos veces sobre el archivo del primary
            if indexes[index]["filename"] == mainfilename:
                continue

            # >>>> R-Tree: NO escribir JSON en el archivo del índice (es binario)
            if indexes[index]["index"] == "rtree":
                # solo asegurar que exista el directorio (el archivo lo manejará RTree.Storage)
                os.makedirs(os.path.dirname(indexes[index]["filename"]), exist_ok=True)
                continue

            # Índices secundarios NO R-Tree: escribir su schema (campo, pk, deleted)
            for schem in new_schema:
                if schem["name"] == index:
                    put_schema.append(schem)

                    # Adjuntar la PK al registro del índice secundario
                    if pk_name is not None:
                        pk_type = new_fields[pk_name]["type"]
                        if "length" in new_fields[pk_name]:
                            put_schema.append({"name": "pk", "type": pk_type, "length": new_fields[pk_name]["length"]})
                        else:
                            put_schema.append({"name": "pk", "type": pk_type})

                    put_schema.append({"name": "deleted", "type": "?"})
                    put_json(indexes[index]["filename"], [put_schema])
                    break

    tables[table] = table_file
    save_tables(tables)
