def build_format(schema):
    # Normaliza schema -> lista de dicts de campo
    if isinstance(schema, dict):
        # ¿me pasaron un solo campo? (tiene name y type)
        if "type" in schema and "name" in schema:
            fields = [schema]
        else:
            # mapping: nombre -> spec
            fields = [{"name": n, **spec} for n, spec in schema.items()]
    elif isinstance(schema, (list, tuple)):
        # lista normal o lista-de-lista
        if len(schema) > 0 and isinstance(schema[0], (list, tuple)):
            fields = list(schema[0])
        else:
            fields = list(schema)
    else:
        raise TypeError(f"Unsupported schema type for build_format: {type(schema)}")

    fmt = ""
    for field in fields:
        t = str(field.get("type", "")).lower()
        length = int(field.get("length", 0) or 0)

        if t in ("i", "int", "integer"):
            fmt += "i"
        elif t in ("h", "smallint"):
            fmt += "h"
        elif t in ("q", "bigint"):
            fmt += "q"
        elif t in ("f", "float", "real"):
            fmt += "f"
        elif t in ("d", "double", "double precision"):
            fmt += "d"
        elif t in ("b", "bool", "boolean", "?"):
            fmt += "?"
        elif t in ("c", "char", "s", "varchar", "string", "text", "blob", "binary", "date", "datetime"):
            if length <= 0:
                length = 1
            fmt += f"{length}s"
        else:
            raise ValueError(f"Unsupported field type in build_format: {field.get('type')}")
    return fmt

def _schema_as_list(schema):
    if isinstance(schema, dict):
        return [{"name": n, **spec} for n, spec in schema.items()]
    if isinstance(schema, (list, tuple)):
        return list(schema[0]) if (len(schema) > 0 and isinstance(schema[0], (list, tuple))) else list(schema)
    raise TypeError(f"Unsupported schema: {type(schema)}")

def _field_spec(schema, name):
    # schema puede ser dict o lista/lista-de-lista
    if isinstance(schema, dict):
        return {"name": name, **schema[name]}
    for f in _schema_as_list(schema):
        if f.get("name") == name:
            return f
    raise KeyError(name)
