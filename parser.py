import re, os, shutil
from typing import List, Dict, Any, Tuple, Optional

from TableCreate import create_table
from Methods import load_tables, save_tables, get_filename, get_json, put_json
from File import File

_NUM = r"[-+]?(?:\d+\.?\d*|\d*\.\d+)"
_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_WS = r"\s+"

_SQL_TYPE_RE = re.compile(rf"^(?P<base>{_IDENT})(?:\s*\((?P<len>\d+)\))?\s*$", re.I)

IDX_ALIASES = {
    "bplus": "b+",
    "b+": "b+",
    "hash": "hash",
    "rtree": "rtree",
    "sequential": "sequential",
    "isam": "isam",
    "heap": "heap",
}

DEFAULT_VARCHAR = 64

class SQLRunner:
    def __init__(self):
        pass

    # ---------- public API ----------
    def execute(self, sql: str) -> Any:
        statements = self._split_statements(sql)
        last = None
        for st in statements:
            st = st.strip()
            if not st:
                continue
            kind = st.split()[0].upper()
            if kind == "CREATE":
                last = self._exec_create(st)
            elif kind == "DROP":
                last = self._exec_drop(st)
            elif kind == "INSERT":
                last = self._exec_insert(st)
            elif kind == "SELECT":
                last = self._exec_select(st)
            elif kind == "DELETE":
                last = self._exec_delete(st)
            else:
                raise ValueError(f"SQL no soportado: {kind}")
        return last

    # ---------- helpers ----------
    def _split_statements(self, sql: str) -> List[str]:
        out, cur, q = [], [], None
        for ch in sql:
            if q:
                cur.append(ch)
                if ch == q:
                    q = None
                continue
            if ch in ("'", '"'):
                q = ch
                cur.append(ch)
            elif ch == ";":
                out.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
        if cur:
            out.append("".join(cur))
        return out

    # ---------- CREATE ----------
    def _exec_create(self, st: str) -> str:
        m_idx = re.match(
            rf"^\s*CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
            rf"(?:(?P<idxname>{_IDENT})\s+)?ON\s+(?P<table>{_IDENT})\s*"
            rf"\(\s*(?P<col>{_IDENT})\s*\)\s*(?:USING\s+(?P<method>[A-Za-z][A-Za-z0-9_\+\-]*))\s*$",
            st, re.I
        )
        if m_idx:
            table = m_idx.group("table")
            col = m_idx.group("col")
            meth = m_idx.group("method")
            if not meth:
                raise ValueError("Falta USING <método> en CREATE INDEX")

            kind = self._norm_index(meth)
            if kind in ("heap", "sequential", "isam"):
                raise ValueError(f"'{kind}' es organización física, no un índice secundario válido")

            main_meta = get_filename(table)
            try:
                relation, indexes = get_json(main_meta, 2)
            except FileNotFoundError:
                raise FileNotFoundError(f"Tabla '{table}' no existe")

            if col not in relation:
                raise ValueError(f"La columna '{col}' no existe en '{table}'")

            if col in indexes and col != "primary":
                if re.search(r"\bIF\s+NOT\s+EXISTS\b", st, re.I):
                    return f"Índice sobre '{table}.{col}' ya existe (ignorado)."
                raise ValueError(f"Índice sobre '{table}.{col}' ya existe")

            prim = indexes.get("primary", {})
            prim_filename = prim.get("filename")
            if not prim_filename:
                raise RuntimeError("No se encontró archivo primario en metadatos")

            hdr = get_json(prim_filename)

            if isinstance(hdr, list) and hdr and isinstance(hdr[0], dict):
                schema_list = hdr
            elif isinstance(hdr, list) and hdr and isinstance(hdr[0], list):
                schema_list = hdr[0]
            else:
                schema_list = hdr if isinstance(hdr, list) else []

            col_l = col.lower()
            col_schema = None
            for ent in schema_list:
                if isinstance(ent, dict) and ent.get("name", "").lower() == col_l:
                    col_schema = dict(ent)  # copia
                    break

            if not col_schema:
                meta = relation[col]
                col_schema = {"name": col, "type": meta.get("type")}
                if "length" in meta:
                    col_schema["length"] = meta["length"]

            table_dir = os.path.dirname(main_meta)
            idx_filename = os.path.join(table_dir, f"{table}-{kind}-{col}.dat")

            put_schema = [col_schema, {"name": "pos", "type": "i"}, {"name": "deleted", "type": "?"}]
            put_json(idx_filename, [put_schema])

            indexes[col] = {"index": kind, "filename": idx_filename}
            put_json(main_meta, [relation, indexes])

            return f"Índice '{kind}' creado sobre '{table}.{col}'."

        m_tab = re.basic operations, some cases missingmatch(rf"^\s*CREATE\s+TABLE\s+({_IDENT})\s*\((.*)\)\s*(?:USING\s+({_IDENT}))?\s*$", st, re.I | re.S)
        if not m_tab:
            raise ValueError("Sintaxis CREATE TABLE inválida")
        table = m_tab.group(1)
        raw_cols = m_tab.group(2)
        table_using = m_tab.group(3).lower() if m_tab.group(3) else None
        if table_using and table_using not in IDX_ALIASES:
            raise ValueError(f"USING {table_using} no soportado")

        cols = self._split_top_level_commas(raw_cols)

        fields: List[Dict[str, Any]] = []
        pending_secondary: List[Tuple[str, str]] = []
        primary_seen = False

        for frag in cols:
            frag = frag.strip()
            if not frag:
                continue

            m_idxcol = re.match(rf"^INDEX\s*\(?\s*({_IDENT})\s*\)?\s+USING\s+([A-Za-z][A-Za-z0-9_\+\-]*)$", frag, re.I)
            if m_idxcol:
                col = m_idxcol.group(1)
                idx = self._norm_index(m_idxcol.group(2))
                if idx in ("heap", "sequential", "isam"):
                    pass
                pending_secondary.append((col, idx))
                continue

            parts = frag.split()
            if len(parts) < 2:
                raise ValueError(f"Definición de columna inválida: {frag}")
            colname = parts[0]

            type_token = []
            i = 1
            while i < len(parts) and parts[i].upper() not in ("PRIMARY", "KEY", "UNIQUE", "INDEX", "USING"):
                type_token.append(parts[i])
                i += 1
            coltype_str = " ".join(type_token)
            ftype, flen = self._parse_type(coltype_str)

            spec = {"name": colname, "type": ftype}
            if flen is not None:
                spec["length"] = flen

            pk = False
            while i < len(parts):
                tok = parts[i].upper()
                if tok == "PRIMARY":
                    if i + 1 < len(parts) and parts[i + 1].upper() == "KEY":
                        pk = True
                        i += 2
                        continue
                if tok == "UNIQUE":
                    spec["key"] = "unique"
                    i += 1
                    continue
                if tok == "INDEX":
                    if i + 2 < len(parts) and parts[i + 1].upper() == "USING":
                        idx = self._norm_index(parts[i + 2])
                        spec["index"] = idx
                        i += 3
                        continue
                if tok == "USING":
                    if i + 1 < len(parts):
                        idx = self._norm_index(parts[i + 1])
                        spec["index"] = idx
                        i += 2
                        continue
                i += 1

            if pk:
                spec["key"] = "primary"
                if "index" not in spec:
                    spec["index"] = IDX_ALIASES.get(table_using or "heap")
                primary_seen = True

            fields.append(spec)

        if table_using and not any(f.get("index") for f in fields if f.get("key") == "primary"):
            for f in fields:
                if f.get("key") == "primary":
                    f["index"] = IDX_ALIASES[table_using]
                    break

        for col, idx in pending_secondary:
            for f in fields:
                if f["name"].lower() == col.lower():
                    f["index"] = idx
                    break
            else:
                raise ValueError(f"Se declaró INDEX para columna inexistente: {col}")

        if not primary_seen:
            raise ValueError("Falta PRIMARY KEY en CREATE TABLE")

        create_table(table, fields)
        return f"Tabla '{table}' creada con {len(fields)} columnas."

    # ---------- DROP ----------
    def _exec_drop(self, st: str) -> str:
        m_tab = re.match(rf"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?({_IDENT})\s*$", st, re.I)
        if m_tab:
            table = m_tab.group(1)

            tables = load_tables()
            exists = table in tables
            if not exists:
                if re.search(r"\bIF\s+EXISTS\b", st, re.I):
                    return f"Tabla '{table}' no existe (ignorada)."
                raise FileNotFoundError(f"Tabla '{table}' no existe")

            files_to_remove = set()
            try:
                main_meta = get_filename(table)
                relation, indexes = get_json(main_meta, 2)
                files_to_remove.add(main_meta)
                for _, meta in (indexes or {}).items():
                    fn = meta.get("filename")
                    if fn:
                        files_to_remove.add(fn)
                table_dir = os.path.dirname(main_meta)
            except Exception:
                table_dir = os.path.join("files", table)

            for fp in list(files_to_remove):
                try: os.remove(fp)
                except FileNotFoundError: pass
                except IsADirectoryError: pass

            try: shutil.rmtree(table_dir)
            except FileNotFoundError: pass

            try:
                del tables[table]
                save_tables(tables)
            except Exception:
                pass

            return f"Tabla '{table}' eliminada."


        m_idx1 = re.match(rf"^\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?({_IDENT})\s+ON\s+({_IDENT})\s*$", st, re.I)
        m_idx2 = re.match(rf"^\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?ON\s+({_IDENT})\s*\(\s*({_IDENT})\s*\)\s*$", st, re.I)

        if m_idx1 or m_idx2:
            if m_idx1:
                col, table = m_idx1.group(1), m_idx1.group(2)
            else:
                table, col = m_idx2.group(1), m_idx2.group(2)

            main_meta = get_filename(table)
            try:
                relation, indexes = get_json(main_meta, 2)
            except FileNotFoundError:
                if re.search(r"\bIF\s+EXISTS\b", st, re.I):
                    return f"Índice '{col}' en '{table}' no existe (ignorado)."
                raise FileNotFoundError(f"Tabla '{table}' no existe")

            if col.lower() == "primary":
                raise ValueError("No se puede eliminar el índice primario (usa DROP TABLE).")

            idx_meta = indexes.get(col)
            if not idx_meta:
                if re.search(r"\bIF\s+EXISTS\b", st, re.I):
                    return f"Índice '{col}' en '{table}' no existe (ignorado)."
                raise FileNotFoundError(f"Índice '{col}' en tabla '{table}' no existe")

            idx_file = idx_meta.get("filename")
            if idx_file:
                try: os.remove(idx_file)
                except FileNotFoundError: pass

            del indexes[col]
            try:
                os.remove(main_meta)
            except FileNotFoundError:
                pass
            put_json(main_meta, [relation, indexes])

            return f"Índice '{col}' eliminado de '{table}'."

        raise ValueError("Sintaxis DROP inválida")

    # ---------- INSERT ----------
    def _exec_insert(self, st: str) -> str:
        m = re.match(
            rf"^\s*INSERT\s+INTO\s+({_IDENT})\s*\(([^)]*)\)\s*VALUES\s*(.+)\s*$",
            st, re.I | re.S
        )
        if not m:
            raise ValueError("Sintaxis INSERT inválida")

        table, cols_raw, values_raw = m.groups()
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
        if not cols:
            raise ValueError("INSERT requiere al menos una columna")

        try:
            main_meta = get_filename(table)
            relation, _indexes = get_json(main_meta, 2)
        except FileNotFoundError:
            raise FileNotFoundError(f"Tabla '{table}' no existe")

        unique_cols = [cname for cname, meta in relation.items() if meta.get("key") in ("primary", "unique")]

        tuples = self._split_value_tuples(values_raw.strip())
        if not tuples:
            raise ValueError("VALUES vacío en INSERT")

        f = File(table)
        n_inserted = 0

        for tup in tuples:
            vals = self._split_top_level_commas(tup)
            if len(cols) != len(vals):
                raise ValueError("El número de columnas y valores no coincide")
            rec: Dict[str, Any] = {}
            for c, v in zip(cols, vals):
                rec[c] = self._parse_literal(v.strip())

            for ucol in unique_cols:
                if ucol in rec:
                    existing = f.execute({"op": "search", "field": ucol, "value": rec[ucol]})
                    if existing:
                        kind = relation[ucol].get("key")
                        etiqueta = "PRIMARY KEY" if kind == "primary" else "UNIQUE"
                        raise ValueError(f"Violación de {etiqueta} en '{ucol}': valor '{rec[ucol]}' ya existe")

            f.execute({"op": "insert", "record": rec})
            n_inserted += 1

        return f"{n_inserted} fila{'s' if n_inserted != 1 else ''} insertada{'s' if n_inserted != 1 else ''}"

    # ---------- SELECT ----------
    def _exec_select(self, st: str):
        m_knn = re.match(rf"^\s*SELECT\s+KNN\s+(\d+)\s+FROM\s+({_IDENT})\s+WHERE\s+({_IDENT})\s+NEAR\s*\(\s*({_NUM})\s*,\s*({_NUM})\s*\)\s*$", st, re.I)
        if m_knn:
            k, table, field, x, y = m_knn.groups()
            k = int(k); x = float(x); y = float(y)
            f = File(table)
            self._say_index_choice(f, op="knn", field=field)
            return f.execute({"op": "knn", "field": field, "point": [x, y], "k": k})

        m_rad = re.match(rf"^\s*SELECT\s+\*\s+FROM\s+({_IDENT})\s+WHERE\s+DISTANCE\s*\(\s*({_IDENT})\s*,\s*\(\s*({_NUM})\s*,\s*({_NUM})\s*\)\s*\)\s*<=\s*({_NUM})\s*$", st, re.I)
        if m_rad:
            table, field, x, y, r = m_rad.groups()
            x = float(x); y = float(y); r = float(r)
            f = File(table)
            self._say_index_choice(f, op="range", field=field)
            return f.execute({"op": "range search", "field": field, "point": [x, y], "r": r})

        m_between = re.match(rf"^\s*SELECT\s+\*\s+FROM\s+({_IDENT})\s+WHERE\s+({_IDENT})\s+BETWEEN\s+({_NUM})\s+AND\s+({_NUM})\s*$", st, re.I)
        if m_between:
            table, field, a, b = m_between.groups()
            a = self._num(a); b = self._num(b)
            f = File(table)
            self._say_index_choice(f, op="range", field=field)
            return f.execute({"op": "range search", "field": field, "min": a, "max": b})

        m_eq = re.match(rf"^\s*SELECT\s+\*\s+FROM\s+({_IDENT})\s+WHERE\s+({_IDENT})\s*=\s*(.+)\s*$", st, re.I)
        if m_eq:
            table, field, val_raw = m_eq.groups()
            val = self._parse_literal(val_raw.strip())
            f = File(table)
            self._say_index_choice(f, op="eq", field=field)
            return f.execute({"op": "search", "field": field, "value": val})

        raise ValueError("Sintaxis SELECT no soportada")

    # ---------- DELETE ----------
    def _exec_delete(self, st: str):
        m = re.match(rf"^\s*DELETE\s+FROM\s+({_IDENT})\s+WHERE\s+({_IDENT})\s*=\s*(.+)\s*$", st, re.I)
        if not m:
            raise ValueError("Sintaxis DELETE inválida")
        table, field, val_raw = m.groups()
        val = self._parse_literal(val_raw.strip())
        f = File(table)
        self._say_index_choice(f, op="delete", field=field)

        res = f.execute({"op": "remove", "field": field, "value": val})

        if isinstance(res, list):
            n = len(res)
        elif isinstance(res, int):
            n = res
        elif res is None:
            n = 0
        else:
            try:
                n = int(res)
            except Exception:
                n = 0

        return f"{n} fila{'s' if n != 1 else ''} eliminada{'s' if n != 1 else ''}"

    # ---------- Utilities ----------
    def _parse_type(self, type_str: str) -> Tuple[str, Optional[int]]:
        m = _SQL_TYPE_RE.match(type_str)
        if not m:
            raise ValueError(f"Tipo SQL inválido: {type_str}")
        base = m.group('base').lower()
        length = m.group('len')
        if base in ("int", "integer", "serial"):
            return "i", None
        if base in ("float", "real"):
            return "f", None
        if base in ("double", "double precision"):
            return "d", None
        if base in ("char", "character"):
            return "c", int(length or DEFAULT_VARCHAR)
        if base in ("varchar", "character varying", "string", "text"):
            return "s", int(length or DEFAULT_VARCHAR)
        if base in ("bool", "boolean"):
            return "?", None
        if base in ("blob", "binary"):
            return "blob", int(length or 256)
        if base in ("date", "datetime"):
            return "date", int(length or 32)
        raise ValueError(f"Tipo no soportado: {base}")

    def _parse_literal(self, token: str):
        token = token.strip()
        if (len(token) >= 2) and token[0] in "'\"" and token[-1] == token[0]:
            return token[1:-1]
        m = re.match(rf"^\(\s*({_NUM})\s*,\s*({_NUM})\s*\)$", token)
        if m:
            return [float(m.group(1)), float(m.group(2))]
        if token.lower() in ("true", "false"):
            return token.lower() == "true"
        m = re.match(rf"^{_NUM}$", token)
        if m:
            v = float(token)
            return int(v) if v.is_integer() else v
        return token

    def _num(self, s: str):
        v = float(s)
        return int(v) if v.is_integer() else v

    def _norm_index(self, s: str) -> str:
        s = s.lower()
        if s not in IDX_ALIASES:
            raise ValueError(f"Índice '{s}' no soportado")
        return IDX_ALIASES[s]

    def _split_top_level_commas(self, s: str) -> List[str]:
        out, cur, depth, q = [], [], 0, None
        for ch in s:
            if q:
                cur.append(ch)
                if ch == q:
                    q = None
                continue
            if ch in ("'", '"'):
                q = ch
                cur.append(ch)
            elif ch == '(':
                depth += 1
                cur.append(ch)
            elif ch == ')':
                depth = max(0, depth - 1)
                cur.append(ch)
            elif ch == ',' and depth == 0:
                out.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
        if cur:
            out.append("".join(cur).strip())
        return out

    def _say_index_choice(self, f: File, op: str, field: str) -> None:
        idxs: Dict[str, Dict[str, Any]] = f.indexes
        choice: Optional[Tuple[str, str]] = None  # (kind, filename)
        field_l = field.lower()
        if field_l in idxs:
            meta = idxs[field_l]
            choice = (meta.get("index", "?"), meta.get("filename", "?"))
        else:
            meta = idxs.get("primary", {})
            choice = (meta.get("index", "?"), meta.get("filename", "?"))
        kind, fname = choice
        print(f"[Planner] Op={op} Campo={field} -> índice '{kind}' en '{fname}'")

    def _split_value_tuples(self, s: str) -> List[str]:
        out, cur, depth, q = [], [], 0, None
        i = 0
        s = s.strip()
        while i < len(s):
            ch = s[i]
            if q:
                cur.append(ch)
                if ch == q:
                    q = None
                i += 1
                continue
            if ch in ("'", '"'):
                q = ch
                cur.append(ch)
            elif ch == "(":
                if depth > 0:
                    cur.append(ch)
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth > 0:
                    cur.append(ch)
                elif depth == 0:
                    out.append("".join(cur).strip())
                    cur = []
            else:
                if depth > 0:
                    cur.append(ch)
            i += 1
        return out


