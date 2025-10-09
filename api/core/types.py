def build_format(schema):
    fmt = ""
    for field in schema:
        ftype = field["type"]
        length = field.get("length", 1)
        t = ftype.lower()

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
        elif t in ("c", "char"):
            fmt += f"{length}s"
        elif t in ("s", "varchar", "string"):
            fmt += f"{length}s"
        elif t in ("b", "bool", "boolean", "?"):
            fmt += "?"
        elif t in ("blob", "binary"):
            fmt += f"{length}s"
        elif t in ("date", "datetime"):
            fmt += f"{length}s"
    return fmt
