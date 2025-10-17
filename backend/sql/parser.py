# parser.py
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Any, Tuple
import re

# ---------------------------
# Tokenizer (lexer)
# ---------------------------

KEYWORDS = {
    "CREATE","TABLE","INDEX","IF","NOT","EXISTS","ON","USING",
    "DROP","INSERT","INTO","VALUES","SELECT","FROM","WHERE",
    "BETWEEN","AND","OR","DELETE","PRIMARY","KEY",
    "INT","INTEGER","SMALLINT","BIGINT","FLOAT","REAL","DOUBLE",
    "PRECISION","CHAR","VARCHAR","STRING","BOOL","BOOLEAN",
    "TRUE","FALSE","NULL","LIKE","IN","IS","AS",
    "FILE","POINT"
}

# operadores que necesitamos en este dialecto
_TWO_CHAR_OPS = {"<=", ">=", "!=", "<>"}
_SINGLE_OPS = set("=<>(),.;+-*")

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUM_RE   = re.compile(r"[-+]?(?:(?:\d+\.\d*)|(?:\d*\.\d+)|(?:\d+))")
_WS_RE    = re.compile(r"\s+")


class Token:
    def __init__(self, kind: str, value: str, pos: int):
        self.kind = kind  # "KW" | "IDENT" | "NUMBER" | "STRING" | "OP"
        self.value = value
        self.pos = pos
    def __repr__(self):
        return f"Token({self.kind},{self.value}@{self.pos})"


def _tokenize(sql: str) -> List[Token]:
    i, n = 0, len(sql)
    out: List[Token] = []
    while i < n:
        m = _WS_RE.match(sql, i)
        if m:
            i = m.end()
            continue

        # operadores de 2 chars primero
        if any(sql.startswith(op, i) for op in _TWO_CHAR_OPS):
            out.append(Token("OP", sql[i:i+2], i))
            i += 2
            continue

        ch = sql[i]

        if ch in _SINGLE_OPS:
            out.append(Token("OP", ch, i))
            i += 1
            continue

        if ch in ("'", '"'):
            q = ch
            j = i + 1
            buf = []
            while j < n:
                c = sql[j]
                if c == q:
                    # escape de comilla duplicada: '' o ""
                    if j + 1 < n and sql[j + 1] == q:
                        buf.append(q)
                        j += 2
                        continue
                    j += 1
                    break
                buf.append(c)
                j += 1
            else:
                raise SyntaxError(f"String sin cerrar desde {i}")
            out.append(Token("STRING", "".join(buf), i))
            i = j
            continue

        m = _NUM_RE.match(sql, i)
        if m:
            out.append(Token("NUMBER", m.group(), i))
            i = m.end()
            continue

        m = _IDENT_RE.match(sql, i)
        if m:
            raw = m.group()
            u = raw.upper()
            if u in KEYWORDS:
                out.append(Token("KW", u, i))
            else:
                out.append(Token("IDENT", raw, i))
            i = m.end()
            continue

        raise SyntaxError(f"Carácter inesperado {sql[i]!r} en {i}")
    return out


# ---------------------------
# AST (dataclasses)
# ---------------------------

@dataclass
class SQLType:
    base: str
    length: Optional[int] = None

@dataclass
class Column:
    name: str
    type: SQLType
    primary_key: bool = False
    pk_using: Optional[str] = None        # PRIMARY KEY USING <metodo/organización>
    inline_index: Optional[str] = None    # INDEX USING <metodo>

@dataclass
class CreateTable:
    kind: str = "create_table"
    name: str = ""
    if_not_exists: bool = False
    columns: List[Column] = field(default_factory=list)
    table_indexes: List[Tuple[str, str]] = field(default_factory=list)  # (col, method)

@dataclass
class CreateIndex:
    kind: str = "create_index"
    if_not_exists: bool = False
    name: Optional[str] = None
    table: str = ""
    column: str = ""
    method: Optional[str] = None

@dataclass
class DropTable:
    kind: str = "drop_table"
    if_exists: bool = False
    name: str = ""

@dataclass
class DropIndex:
    kind: str = "drop_index"
    if_exists: bool = False
    name: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None

@dataclass
class Insert:
    kind: str = "insert"
    table: str = ""
    columns: Optional[List[str]] = None
    values: Optional[List[Any]] = None          # cuando viene un solo VALUES(...)
    rows: List[List[Any]] = field(default_factory=list)  # VALUES(...),(...),...
    from_file: Optional[str] = None             # opcional: INSERT INTO t FROM FILE 'path'

@dataclass
class Comparison:
    left: Any
    op: str
    right: Any

@dataclass
class Between:
    ident: str
    lo: Any
    hi: Any

@dataclass
class BoolExpr:
    op: str         # "AND" | "OR"
    items: List[Any]

@dataclass
class Select:
    kind: str = "select"
    table: str = ""
    columns: Optional[List[str]] = None   # None => "*"
    where: Optional[Any] = None

@dataclass
class Delete:
    kind: str = "delete"
    table: str = ""
    where: Optional[Any] = None

@dataclass
class CreateTableFromFile:
    kind: str = "create_table_from_file"
    name: str = ""
    path: str = ""
    if_not_exists: bool = False
    index_method: Optional[str] = None   # p.ej. "isam", "bplus", etc.
    index_column: Optional[str] = None   # p.ej. "id"

@dataclass
class InList:
    ident: str
    items: List[Any]

@dataclass
class GeoWithin:
    """ubicacion IN (POINT(x,y), r)"""
    ident: str
    center: Any      # dict {"kind":"point","x":..,"y":..} o literal/ident
    radius: Any      # número o literal

# ---------------------------
# Parser (recursive descent)
# ---------------------------

class _Parser:
    def __init__(self, toks: List[Token]):
        self.toks = toks
        self.i = 0

    # helpers
    def _peek(self) -> Optional[Token]:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def _peek_is(self, kind=None, value=None) -> bool:
        t = self._peek()
        if not t:
            return False
        if kind and t.kind != kind:
            return False
        if value and t.value != value:
            return False
        return True

    def _accept(self, kind=None, value=None) -> Optional[Token]:
        if self._peek_is(kind, value):
            t = self._peek()
            self.i += 1
            return t
        return None

    def _expect(self, kind=None, value=None) -> Token:
        t = self._accept(kind, value)
        if not t:
            exp = value or kind or "token"
            pos = self._peek().pos if self._peek() else "EOF"
            raise SyntaxError(f"Se esperaba {exp} en {pos}")
        return t

    # entrypoint
    def parse(self):
        out = []
        while self._peek():
            out.append(self._parse_statement())
            self._accept("OP", ";")
        return out

    def _parse_statement(self):
        t = self._peek()
        if not t or t.kind != "KW":
            raise SyntaxError("Se esperaba una sentencia")
        if t.value == "CREATE":
            return self._parse_create()
        if t.value == "DROP":
            return self._parse_drop()
        if t.value == "INSERT":
            return self._parse_insert()
        if t.value == "SELECT":
            return self._parse_select()
        if t.value == "DELETE":
            return self._parse_delete()
        raise SyntaxError(f"Sentencia no soportada: {t.value}")

    # CREATE
    def _parse_create(self):
        self._expect("KW", "CREATE")
        if self._accept("KW", "TABLE"):
            return self._parse_create_table()
        if self._accept("KW", "INDEX"):
            return self._parse_create_index()
        raise SyntaxError("CREATE debe ser TABLE o INDEX")

    def _parse_create_index(self):
        if_not_exists = False
        if self._accept("KW", "IF"):
            self._expect("KW", "NOT"); self._expect("KW", "EXISTS")
            if_not_exists = True

        name = None
        if self._peek_is("KW", "ON"):
            self._expect("KW", "ON")
            table = self._parse_ident()
        else:
            name = self._parse_ident()
            self._expect("KW", "ON")
            table = self._parse_ident()

        self._expect("OP", "(")
        col = self._parse_ident()
        self._expect("OP", ")")

        method = None
        if self._accept("KW", "USING"):
            method = self._parse_method_token()
        return CreateIndex(if_not_exists=if_not_exists, name=name, table=table, column=col, method=method)

    def _parse_create_table(self):
        if_not_exists = False
        if self._accept("KW", "IF"):
            self._expect("KW", "NOT"); self._expect("KW", "EXISTS")
            if_not_exists = True

        name = self._parse_ident()

        # CREATE TABLE t FROM [FILE] 'path' [USING INDEX <method>(col)]
        if self._accept("KW", "FROM"):
            self._accept("KW", "FILE")  # opcional
            path = self._expect("STRING").value

            index_method = None
            index_column = None
            if self._accept("KW", "USING"):
                self._expect("KW", "INDEX")
                index_method = self._parse_method_token()
                self._expect("OP", "(")
                col_tok = self._peek()
                if col_tok and col_tok.kind in {"IDENT", "KW", "STRING"}:
                    index_column = self._parse_literal()
                else:
                    raise SyntaxError("Se esperaba nombre de columna del índice")
                self._expect("OP", ")")

            return CreateTableFromFile(
                name=name,
                path=path,
                if_not_exists=if_not_exists,
                index_method=index_method,
                index_column=index_column
            )

        self._expect("OP", "(")

        cols: List[Column] = []
        table_indexes: List[Tuple[str, str]] = []

        while True:
            if self._peek_is("OP", ")"):
                self._expect("OP", ")")
                break

            # INDEX(col) USING method  (a nivel de tabla)
            if self._peek_is("KW", "INDEX"):
                self._expect("KW", "INDEX")
                self._expect("OP", "(")
                colname = self._parse_ident()
                self._expect("OP", ")")
                self._expect("KW", "USING")
                method = self._parse_method_token()
                table_indexes.append((colname, method))
            else:
                # definición de columna
                colname = self._parse_ident()
                typ = self._parse_type()
                c = Column(name=colname, type=typ)

                # 0..N restricciones inline
                while True:
                    pos = self.i
                    try:
                        c = self._parse_col_constraint(c)
                    except SyntaxError:
                        self.i = pos
                    if self.i == pos:
                        break
                cols.append(c)

            if self._accept("OP", ","):
                continue
            elif self._peek_is("OP", ")"):
                continue
            else:
                where = self._peek().pos if self._peek() else "EOF"
                raise SyntaxError(f"Se esperaba ',' o ')' en la lista de columnas en {where}")

        return CreateTable(name=name, if_not_exists=if_not_exists, columns=cols, table_indexes=table_indexes)

    def _parse_type(self) -> SQLType:
        base = self._parse_ident()
        length = None
        if self._accept("OP", "("):
            num = self._expect("NUMBER").value
            try:
                length = int(float(num))
            except ValueError:
                raise SyntaxError("La longitud del tipo debe ser entera")
            self._expect("OP", ")")
        return SQLType(base=base, length=length)

    def _parse_col_constraint(self, col: Column) -> Column:
        # PRIMARY KEY [USING <org>]
        if self._accept("KW", "PRIMARY"):
            self._expect("KW", "KEY")
            col.primary_key = True
            if self._accept("KW", "USING"):
                col.pk_using = self._parse_method_token()
            return col
        # INDEX USING <método>
        if self._accept("KW", "INDEX"):
            self._expect("KW", "USING")
            col.inline_index = self._parse_method_token()
            return col
        # nada
        raise SyntaxError("")

    def _parse_method_token(self) -> str:
        # Acepta tokens tipo: b+, bplus, r-tree, etc. (no valida, solo concatena IDENT/KW y + -)
        parts = []
        t = self._peek()
        while t and (t.kind in {"IDENT", "KW"} or (t.kind == "OP" and t.value in {"+", "-"})):
            parts.append(t.value)
            self.i += 1
            t = self._peek()
        if not parts:
            where = self._peek().pos if self._peek() else "EOF"
            raise SyntaxError(f"Se esperaba nombre de método en {where}")
        return "".join(parts)

    def _parse_ident(self) -> str:
        t = self._peek()
        # Permitimos KW como identificadores cuando no son palabras clave estructurales
        if t and (t.kind == "IDENT" or (t.kind == "KW" and t.value not in {
            "SELECT","FROM","WHERE","AND","OR","VALUES","CREATE","DROP","DELETE","INSERT","INTO"
        })):
            self.i += 1
            return t.value
        where = t.pos if t else "EOF"
        raise SyntaxError(f"Se esperaba identificador en {where}")

    def _parse_number_like(self, v: str):
        if "." in v or "e" in v.lower():
            return float(v)
        return int(v)

    def _parse_point(self) -> dict:
        # POINT(<num>, <num>)
        self._expect("KW", "POINT")
        self._expect("OP", "(")
        tx = self._expect("NUMBER").value
        self._expect("OP", ",")
        ty = self._expect("NUMBER").value
        self._expect("OP", ")")
        return {"kind": "point", "x": self._parse_number_like(tx), "y": self._parse_number_like(ty)}

    # DROP
    def _parse_drop(self):
        self._expect("KW", "DROP")
        if self._accept("KW", "TABLE"):
            if_exists = bool(self._accept("KW", "IF") and self._expect("KW", "EXISTS"))
            name = self._parse_ident()
            return DropTable(if_exists=if_exists, name=name)
        if self._accept("KW", "INDEX"):
            if_exists = bool(self._accept("KW", "IF") and self._expect("KW", "EXISTS"))
            # Formas: DROP INDEX name [ON table]  |  DROP INDEX ON table (col)
            if self._accept("KW", "ON"):
                table = self._parse_ident()
                self._expect("OP", "(")
                column = self._parse_ident()
                self._expect("OP", ")")
                return DropIndex(if_exists=if_exists, table=table, column=column)
            else:
                name = self._parse_ident()
                table = None
                if self._accept("KW", "ON"):
                    table = self._parse_ident()
                return DropIndex(if_exists=if_exists, name=name, table=table)
        raise SyntaxError("DROP debe ser TABLE o INDEX")

    # INSERT
    def _parse_insert(self):
        self._expect("KW", "INSERT")
        self._expect("KW", "INTO")
        table = self._parse_ident()

        cols = None
        if self._accept("OP", "("):
            cols = []
            while True:
                cols.append(self._parse_ident())
                if self._accept("OP", ")"):
                    break
                self._expect("OP", ",")

        # INSERT INTO t FROM [FILE] 'path'
        if self._accept("KW", "FROM"):
            self._accept("KW", "FILE")  # opcional
            path = self._expect("STRING").value
            return Insert(table=table, columns=cols, from_file=path)

        self._expect("KW", "VALUES")

        rows = []
        while True:
            self._expect("OP", "(")
            values = []
            values.append(self._parse_literal())
            while self._accept("OP", ","):
                values.append(self._parse_literal())
            self._expect("OP", ")")
            rows.append(values)
            if not self._accept("OP", ","):
                break
        single = rows[0] if len(rows) == 1 else None
        return Insert(table=table, columns=cols, values=single, rows=rows)

    # SELECT
    def _parse_select(self):
        self._expect("KW", "SELECT")
        cols = None
        if self._accept("OP", "*"):
            cols = None
        else:
            cols = [self._parse_ident()]
            while self._accept("OP", ","):
                cols.append(self._parse_ident())

        self._expect("KW", "FROM")
        table = self._parse_ident()

        where = None
        if self._accept("KW", "WHERE"):
            where = self._parse_expr()
        return Select(table=table, columns=cols, where=where)

    # WHERE expression
    def _parse_expr(self):
        left = self._parse_term()
        while self._accept("KW", "OR"):
            right = self._parse_term()
            left = BoolExpr(op="OR", items=[left, right])
        return left

    def _parse_term(self):
        left = self._parse_factor()
        while self._accept("KW", "AND"):
            right = self._parse_factor()
            left = BoolExpr(op="AND", items=[left, right])
        return left

    def _parse_factor(self):
        if self._accept("OP", "("):
            e = self._parse_expr()
            self._expect("OP", ")")
            return e

        ident = self._parse_ident()
        if self._accept("KW", "BETWEEN"):
            lo = self._parse_literal()
            self._expect("KW", "AND")
            hi = self._parse_literal()
            return Between(ident=ident, lo=lo, hi=hi)

        if self._accept("KW", "IN"):
            self._expect("OP", "(")
            # ¿POINT(x,y) o primer literal?
            first_is_point = False
            t = self._peek()
            if t and t.kind == "KW" and t.value == "POINT":
                center = self._parse_point()
                first_is_point = True
            else:
                center = self._parse_literal()

            # Si hay coma, intentamos distinguir círculo vs. lista
            if self._accept("OP", ","):
                radius = self._parse_literal()
                self._expect("OP", ")")
                if first_is_point or (isinstance(center, dict) and center.get("kind") == "point"):
                    return GeoWithin(ident=ident, center=center, radius=radius)
                else:
                    return InList(ident=ident, items=[center, radius])

            # Lista IN con un solo valor (o varios)
            items = [center]
            while self._accept("OP", ","):
                items.append(self._parse_literal())
            self._expect("OP", ")")
            return InList(ident=ident, items=items)

        op_tok = self._expect("OP")
        if op_tok.value not in {"=", "!=", "<>", "<", "<=", ">", ">="}:
            raise SyntaxError(f"Operador no válido: {op_tok.value}")
        lit = self._parse_literal()
        return Comparison(left=ident, op=op_tok.value, right=lit)

    def _parse_literal(self):
        t = self._peek()
        if not t:
            raise SyntaxError("Se esperaba literal")
        if t.kind == "NUMBER":
            self.i += 1
            s = t.value
            if "." in s or "e" in s.lower():
                return float(s)
            return int(s)
        if t.kind == "STRING":
            self.i += 1
            return t.value
        if t.kind == "KW" and t.value in {"TRUE", "FALSE", "NULL"}:
            self.i += 1
            return True if t.value == "TRUE" else (False if t.value == "FALSE" else None)
        # permitimos identificadores como literales “strings”
        if t.kind in {"IDENT", "KW"}:
            self.i += 1
            return t.value
        raise SyntaxError(f"Se esperaba literal en {t.pos}")

    # --- DELETE ---
    def _parse_delete(self):
        self._expect("KW", "DELETE")
        self._expect("KW", "FROM")
        table = self._parse_ident()
        where = None
        if self._accept("KW", "WHERE"):
            ident = self._parse_ident()
            op_tok = self._expect("OP")
            if op_tok.value not in {"=", "!=", "<>", "<", "<=", ">", ">="}:
                raise SyntaxError(f"Operador no válido en DELETE: {op_tok.value}")
            lit = self._parse_literal()
            where = {"op": op_tok.value, "left": ident, "right": lit}
        try:
            return Delete(table=table, where=where)
        except NameError:
            return {"kind": "delete", "table": table, "where": where}


# ---------------------------
# API de alto nivel
# ---------------------------

def parse_sql(sql: str):
    toks = _tokenize(sql)
    ast = _Parser(toks).parse()
    return ast

class SQLParser:
    def parse(self, sql: str):
        return parse_sql(sql)

class SQLRunner:
    """
    Mantengo el nombre 'SQLRunner' por compatibilidad, pero
    **solo parsea** y retorna ASTs (como dicts), sin ejecutar.
    """
    def execute(self, sql: str):
        ast = parse_sql(sql)
        return [asdict(s) for s in ast]
