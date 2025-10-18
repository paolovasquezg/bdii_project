from backend.storage.indexes.heap import HeapFile
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import os, struct, io

DEBUG_IDX = os.getenv("BD2_DEBUG_INDEX", "0").lower() in ("1", "true", "yes")

MBR = Tuple[float, float, float, float]
RID = Tuple[int, int]

@dataclass
class Entry:
    mbr: MBR
    child: Optional[int] = None
    rid: Optional[RID] = None

@dataclass
class Node:
    page_id: int
    is_leaf: bool
    entries: List[Entry] = field(default_factory=list)
    M: int = 32

    def mbr_cover(self) -> MBR:
        m = self.entries[0].mbr
        for e in self.entries[1:]:
            m = expand(m, e.mbr)
        return m

    def full(self) -> bool:
        return len(self.entries) >= self.M


class RTreeFile:
    def __init__(self, filename: str, M: int = 32):
        self.store = Storage(filename, M=M)
        self.opened = False

    # ---------- lifecycle ----------
    def open(self):
        if not self.opened:
            self.store.open()
            if self.store.root == 0:
                # Check if root page already exists and has content
                try:
                    root = self.store.read_node(0)
                    if len(root.entries) > 0:
                        # Root already populated, don't overwrite
                        pass
                    else:
                        # Root exists but is empty, initialize it
                        root = Node(page_id=0, is_leaf=True, M=self.store.M)
                        self.store.write_node(root)
                except Exception:
                    # Root doesn't exist or error, create new
                    root = Node(page_id=0, is_leaf=True, M=self.store.M)
                    self.store.write_node(root)
            self.opened = True

    def close(self):
        if self.opened:
            self.store.close()
            self.opened = False

    # ---------- helpers ----------
    def _choose_leaf(self, page_id: int, obj_mbr: MBR) -> Node:
        node = self.store.read_node(page_id)
        if node.is_leaf:
            return node
        best = None; best_enl = None; best_area = None
        for e in node.entries:
            enl = enlargement(e.mbr, obj_mbr)
            area_e = (e.mbr[1]-e.mbr[0]) * (e.mbr[3]-e.mbr[2])
            if best is None or enl < best_enl or (enl == best_enl and area_e < best_area):
                best, best_enl, best_area = e, enl, area_e
        return self._choose_leaf(best.child, obj_mbr)

    def _cover(self, group: List[Entry]) -> MBR:
        m = group[0].mbr
        for e in group[1:]:
            m = expand(m, e.mbr)
        return m

    def _split_linear(self, entries: List[Entry]):
        xs = [(e.mbr[0], e.mbr[1]) for e in entries]
        ys = [(e.mbr[2], e.mbr[3]) for e in entries]
        span_x = (max(b for _, b in xs) - min(a for a, _ in xs))
        span_y = (max(b for _, b in ys) - min(a for a, _ in ys))
        if span_x >= span_y:
            left = min(entries, key=lambda e: e.mbr[0])
            right = max(entries, key=lambda e: e.mbr[1])
        else:
            left = min(entries, key=lambda e: e.mbr[2])
            right = max(entries, key=lambda e: e.mbr[3])
        g1 = [left]; g2 = [right] if right is not left else []
        for e in entries:
            if e in (left, right): continue
            enl1 = enlargement(self._cover(g1), e.mbr)
            enl2 = enlargement(self._cover(g2), e.mbr)
            if enl1 <= enl2: g1.append(e)
            else: g2.append(e)
        return g1, g2

    def _adjust_tree_after_split(self, path, old: Node, new: Node):
        if not path:
            new_root_id = self.store.alloc_page()
            root = Node(page_id=new_root_id, is_leaf=False, M=self.store.M)
            root.entries = [
                Entry(mbr=old.mbr_cover(), child=old.page_id),
                Entry(mbr=new.mbr_cover(), child=new.page_id),
            ]
            self.store.root = new_root_id
            self.store.height += 1
            self.store.write_node(old); self.store.write_node(new); self.store.write_node(root)
            return

        parent_id, parent = path.pop()
        for i, pe in enumerate(parent.entries):
            if pe.child == old.page_id:
                parent.entries[i].mbr = old.mbr_cover()
                break
        parent.entries.append(Entry(mbr=new.mbr_cover(), child=new.page_id))

        if len(parent.entries) > self.store.M:
            e_all = parent.entries[:]
            g1, g2 = self._split_linear(e_all)
            parent.entries = g1
            new_parent_id = self.store.alloc_page()
            new_parent = Node(page_id=new_parent_id, is_leaf=False, M=self.store.M)
            new_parent.entries = g2
            self.store.write_node(old); self.store.write_node(new)
            self.store.write_node(parent); self.store.write_node(new_parent)
            self._adjust_tree_after_split(path, parent, new_parent)
        else:
            self.store.write_node(old); self.store.write_node(new); self.store.write_node(parent)

    # ---------- public ops ----------
    def insert(self, record: dict, additional: dict) -> List[dict]:
        """Inserta una entrada en el R-Tree.

        Soporta dos modalidades de identificador en hojas:
        - Primario HEAP: record['pos'] (y opcional record['slot'])
        - Primario NO-HEAP: record['pk'] (slot=0)

        La columna espacial debe venir en additional['key'].
        """
        self.open()
        key = additional["key"]        # ej. "ubicacion"
        val = record[key]
        if DEBUG_IDX: print(f"[RTREE insert] key={key} val={val} record={record}")
        if isinstance(val, (list, tuple)) and len(val) == 2:
            m = from_point(float(val[0]), float(val[1]))
        elif isinstance(val, (list, tuple)) and len(val) == 4:
            m = (float(val[0]), float(val[1]), float(val[2]), float(val[3]))
        else:
            raise ValueError("Valor espacial inválido; esperado [x,y] o [xmin,xmax,ymin,ymax].")

        # bajar hasta hoja guardando el path
        path = []
        cur = self.store.root
        while True:
            node = self.store.read_node(cur)
            if node.is_leaf: break
            best = min(node.entries, key=lambda e: enlargement(e.mbr, m))
            path.append((cur, node))
            cur = best.child

        # insertar en hoja
        # Determinar identificador: posición de heap o pk (no-heap)
        if "pos" in record and record["pos"] is not None:
            pos = int(record["pos"])  # posición en heap
            slot = int(record.get("slot", 0))
        elif "pk" in record and record["pk"] is not None:
            # Guardamos pk en el campo 'page' del RID y usamos slot=0
            try:
                pos = int(record["pk"])  # PK numérica requerida
            except Exception as e:
                raise ValueError("RTreeFile.insert: 'pk' debe ser numérica para primarios no-heap") from e
            slot = 0
        else:
            raise KeyError("pos")  # mantener compat con manejo actual en capas superiores

        node.entries.append(Entry(mbr=m, rid=(pos, slot)))
        if len(node.entries) <= self.store.M:
            self.store.write_node(node)
            # actualizar MBRs hacia arriba
            while path:
                pid, parent = path.pop()
                for i, e in enumerate(parent.entries):
                    if e.child == node.page_id:
                        parent.entries[i].mbr = node.mbr_cover()
                        break
                self.store.write_node(parent)
                node = parent
            return [record]

        # split hoja
        e_all = node.entries[:]
        g1, g2 = self._split_linear(e_all)
        node.entries = g1
        new_id = self.store.alloc_page()
        new_node = Node(page_id=new_id, is_leaf=True, M=self.store.M)
        new_node.entries = g2
        self._adjust_tree_after_split(path, node, new_node)
        return [record]

    # --- util para leer del heap si está disponible ---
    def _maybe_heap(self, additional):
        heap = None
        hp = additional.get("heap") if additional else None
        if hp:
            try:
                heap = HeapFile(hp)
            except Exception:
                heap = None
        return heap

    def search(self, additional: dict) -> List[dict]:
        """Intersección con rect: additional['rect']=(xmin,xmax,ymin,ymax), optional 'heap'."""
        self.open()
        rect: MBR = tuple(map(float, additional["rect"]))  # type: ignore
        heap = self._maybe_heap(additional)
        out: List[dict] = []
        stack = [self.store.root]
        while stack:
            pid = stack.pop()
            node = self.store.read_node(pid)
            if node.is_leaf:
                for e in node.entries:
                    if intersects(e.mbr, rect):
                        pos, slot = e.rid
                        if heap and hasattr(heap, "search_by_pos"):
                            rec = heap.search_by_pos(pos)
                            if rec: out.append(rec)
                        else:
                            out.append({"pos": pos, "slot": slot, "mbr": e.mbr})
            else:
                for e in node.entries:
                    if intersects(e.mbr, rect):
                        stack.append(e.child)
        return out

    def range_search(self, additional: dict) -> List[dict]:
        """point + radio: intersecta con rect circunscrito y filtra por distancia real."""
        self.open()
        x, y = additional["point"]
        r = float(additional.get("r", additional.get("radio")))
        rect = rect_from_point_radius(float(x), float(y), r)
        r2 = r * r
        heap = self._maybe_heap(additional)
        out: List[dict] = []
        stack = [self.store.root]
        while stack:
            pid = stack.pop()
            node = self.store.read_node(pid)
            if node.is_leaf:
                for e in node.entries:
                    if intersects(e.mbr, rect):
                        cx = (e.mbr[0] + e.mbr[1]) / 2.0
                        cy = (e.mbr[2] + e.mbr[3]) / 2.0
                        if (cx - x)**2 + (cy - y)**2 <= r2:
                            pos, slot = e.rid
                            if heap and hasattr(heap, "search_by_pos"):
                                rec = heap.search_by_pos(pos)
                                if rec: out.append(rec)
                            else:
                                out.append({"pos": pos, "slot": slot, "mbr": e.mbr})
            else:
                for e in node.entries:
                    if intersects(e.mbr, rect):
                        stack.append(e.child)
        return out

    def knn(self, additional: dict) -> List[dict]:
        """k vecinos más cercanos usando best-first con MINDIST."""
        import heapq
        self.open()
        x, y = map(float, additional["point"]); k = int(additional["k"])
        heap_in = self._maybe_heap(additional)
        pq = []  # Cola de prioridad
        # Inicializamos con el nodo raíz (distancia = 0)
        heapq.heappush(pq, (0.0, False, self.store.root, -1))
        results: List[Tuple[float, dict]] = []
        while pq and len(results) < k:
            dist2, is_leaf, pid, _ = heapq.heappop(pq)
            node = self.store.read_node(pid)
            if node.is_leaf:
                for e in node.entries:
                    cx = (e.mbr[0] + e.mbr[1]) / 2.0
                    cy = (e.mbr[2] + e.mbr[3]) / 2.0
                    d2 = (cx - x)**2 + (cy - y)**2
                    if heap_in and hasattr(heap_in, "search_by_pos"):
                        rec = heap_in.search_by_pos(e.rid[0])
                        if rec: results.append((d2, rec))
                    else:
                        results.append((d2, {"pos": e.rid[0], "slot": e.rid[1], "mbr": e.mbr}))
                results.sort(key=lambda t: t[0])
                results = results[:k]
            else:
                for e in node.entries:
                    d2 = mindist_point_mbr(x, y, e.mbr)
                    heapq.heappush(pq, (d2, False, e.child, -1))
        return [rec for _, rec in results]  # Retorna solo los registros

    # ---------- helpers para delete ----------
    def _eq_mbr(self, a: MBR, b: MBR, eps: float = 1e-9) -> bool:
        return (abs(a[0]-b[0]) <= eps and abs(a[1]-b[1]) <= eps and
                abs(a[2]-b[2]) <= eps and abs(a[3]-b[3]) <= eps)

    def _find_leaf_with(self, page_id: int, rid: RID, target_mbr: Optional[MBR], path: list):
        """
        DFS hasta una hoja que contenga la entrada con 'rid'
        y (opcionalmente) MBR==target_mbr.
        Devuelve (leaf_node, idx_en_leaf, path_copiado) o (None,None,None).
        'path' acumula [(pid, node), ...] desde la raíz hasta el padre del 'page_id' actual.
        """
        node = self.store.read_node(page_id)
        if node.is_leaf:
            for i, e in enumerate(node.entries):
                if e.rid == rid:
                    if (target_mbr is None) or self._eq_mbr(e.mbr, target_mbr):
                        return node, i, list(path)
            return None, None, None
        # interno: explorar hijos que puedan contenerlo (por MBR si se conoce)
        for e in node.entries:
            if (target_mbr is None) or intersects(e.mbr, target_mbr):
                path.append((page_id, node))
                leaf, idx, found_path = self._find_leaf_with(e.child, rid, target_mbr, path)
                path.pop()
                if leaf is not None:
                    return leaf, idx, found_path
        return None, None, None

    # ---------- delete ----------
    def remove(self, additional: dict):
        """
        Elimina una entrada por rid=(pos,slot) y opcionalmente verificando MBR ('mbr').
        Ajusta MBRs hacia arriba. Caso simple:
          - Si hoja queda vacía: se borra la referencia en el padre.
          - Si la raíz (no hoja) queda con un solo hijo: se "comprime" promoviendo al hijo.
        Retorna 1 si borró, 0 si no encontró.
        """
        self.open()
        rid: RID = additional.get("rid")
        if not rid or rid[0] is None:
            raise ValueError("RTreeFile.remove: falta 'rid' (pos,slot)")
        target_mbr: Optional[MBR] = additional.get("mbr")

        # 1) localizar hoja y posición
        leaf, idx, path = self._find_leaf_with(self.store.root, rid, target_mbr, [])
        if leaf is None:
            return 0  # no encontrado

        # 2) borrar en la hoja
        del leaf.entries[idx]
        self.store.write_node(leaf)

        # 3) actualizar hacia arriba (y remover hijos vacíos)
        child = leaf
        while path:
            pid, parent = path.pop()
            # refrescar el parent desde disco por si ya cambió antes:
            parent = self.store.read_node(pid)

            # localizar entrada que apunta al 'child'
            pos = None
            for i, e in enumerate(parent.entries):
                if e.child == child.page_id:
                    pos = i; break

            if pos is None:
                # nada que actualizar (inconsistencia rara); continuar
                child = parent
                continue

            if child.entries:
                # actualizar MBR del hijo
                parent.entries[pos].mbr = child.mbr_cover()
            else:
                # hijo quedó vacío: quitar la entrada que lo referencia
                parent.entries.pop(pos)

            self.store.write_node(parent)
            child = parent  # seguir subiendo

        # 4) comprimir raíz si es interno con un solo hijo
        root = self.store.read_node(self.store.root)
        if not root.is_leaf:
            if len(root.entries) == 0:
                # árbol vacío ⇒ raíz pasa a ser hoja vacía
                # (dejamos root como está pero sin entradas)
                pass
            elif len(root.entries) == 1:
                only_child = root.entries[0].child
                # promover hijo como nueva raíz
                self.store.root = only_child
                self.store.height = max(1, self.store.height - 1)
                # no es necesario escribir el nodo antiguo de raíz; el header se persiste en close()

        return 1

    def stats(self) -> dict:
        return basic_stats(self.store.height, -1, self.store.read_count, self.store.write_count, 0.0)



# Header binario del archivo R-Tree
HEADER_FMT = "<4sBHHIHIQQ"  # magic,ver,M,m,root,height,page_size,read_count,write_count
MAGIC = b"RTRE"
VERSION = 1
DEFAULT_PAGE_SIZE = 4096

class Storage:
    def __init__(self, filename: str, M: int = 32):
        self.filename = filename
        self.M = M
        self.m = max(2, M // 2)
        self.page_size = DEFAULT_PAGE_SIZE
        self.root = 0
        self.height = 1

        # --- contadores internos (totales) + baseline para reportar delta ---
        self._r_total = 0
        self._w_total = 0
        self._r_reported = 0
        self._w_reported = 0

    # --- propiedades: exponen DELTA y permiten setear TOTALES al cargar ---
    @property
    def read_count(self) -> int:
        delta = self._r_total - self._r_reported
        self._r_reported = self._r_total
        return delta

    @read_count.setter
    def read_count(self, v: int):
        self._r_total = int(v or 0)
        self._r_reported = self._r_total

    @property
    def write_count(self) -> int:
        delta = self._w_total - self._w_reported
        self._w_reported = self._w_total
        return delta

    @write_count.setter
    def write_count(self, v: int):
        self._w_total = int(v or 0)
        self._w_reported = self._w_total

    def open(self):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as f:
                # archivo nuevo: persiste totales 0
                f.write(struct.pack(
                    HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                    0, self.height, self.page_size,
                    0, 0
                ))
                f.write(b"\x00" * self.page_size)  # página 0
            # baseline 0
            self.read_count = 0
            self.write_count = 0
        else:
            with open(self.filename, "rb") as f:
                hdr = f.read(struct.calcsize(HEADER_FMT))
                try:
                    magic, ver, M, m, root, height, page_size, r, w = struct.unpack(HEADER_FMT, hdr)
                except struct.error:
                    magic, ver = b"????", -1  # archivo sucio
            # si el archivo estaba sucio (por ej., JSON), recrear
            if magic != MAGIC or ver != VERSION:
                with open(self.filename, "wb") as f:
                    self.root = 0
                    self.height = 1
                    # reinicia totales a 0
                    self.read_count = 0
                    self.write_count = 0
                    f.write(struct.pack(
                        HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                        0, self.height, self.page_size,
                        0, 0
                    ))
                    f.write(b"\x00" * self.page_size)
            else:
                self.M, self.m, self.root, self.height = M, m, root, height
                self.page_size = page_size
                # inicializa TOTALES (+ baseline) desde header
                self.read_count = r
                self.write_count = w

    def close(self):
        if DEBUG_IDX:
            print(f"[Storage.close] root={self.root} height={self.height} filename={self.filename}")
        with open(self.filename, "r+b") as f:
            f.seek(0)
            # persiste TOTALES (no el delta)
            f.write(struct.pack(
                HEADER_FMT, MAGIC, VERSION, self.M, self.m,
                self.root, self.height, self.page_size,
                self._r_total, self._w_total
            ))
        if DEBUG_IDX:
            print(f"[Storage.close] wrote header with root={self.root} height={self.height}")

    # ---- páginas
    def alloc_page(self) -> int:
        size = os.path.getsize(self.filename)
        header = struct.calcsize(HEADER_FMT)
        used_pages = (size - header) // self.page_size
        with open(self.filename, "ab") as f:
            f.write(b"\x00" * self.page_size)
        # cuenta la escritura de la nueva página
        self._w_total += 1
        return used_pages

    # ---- nodos
    def write_node(self, node: Node):
        buf = io.BytesIO()
        buf.write(struct.pack("<B H", 1 if node.is_leaf else 0, len(node.entries)))
        for e in node.entries:
            buf.write(struct.pack("<ffff", *e.mbr))
            if node.is_leaf:
                page, slot = e.rid
                buf.write(struct.pack("<IH", page, slot))
            else:
                buf.write(struct.pack("<I", e.child))
        data = buf.getvalue()
        if len(data) > self.page_size:
            raise ValueError("Node overflow page_size (reduce M).")
        data += b"\x00" * (self.page_size - len(data))
        with open(self.filename, "r+b") as f:
            f.seek(struct.calcsize(HEADER_FMT) + node.page_id * self.page_size)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())  # Force OS to write to disk
        # contar escritura de página de nodo
        self._w_total += 1
        if DEBUG_IDX and node.is_leaf:
            print(f"[write_node] wrote leaf page_id={node.page_id} entries={len(node.entries)}")

    def read_node(self, page_id: int) -> Node:
        with open(self.filename, "rb") as f:
            f.seek(struct.calcsize(HEADER_FMT) + page_id * self.page_size)
            raw = f.read(self.page_size)
        # contar lectura de página de nodo
        self._r_total += 1
        is_leaf, count = struct.unpack_from("<B H", raw, 0)
        off = 3
        entries: List[Entry] = []
        for _ in range(count):
            xmin, xmax, ymin, ymax = struct.unpack_from("<ffff", raw, off); off += 16
            if is_leaf:
                page, slot = struct.unpack_from("<IH", raw, off); off += 6
                entries.append(Entry(mbr=(xmin, xmax, ymin, ymax), rid=(page, slot)))
            else:
                child, = struct.unpack_from("<I", raw, off); off += 4
                entries.append(Entry(mbr=(xmin, xmax, ymin, ymax), child=child))
        return Node(page_id=page_id, is_leaf=bool(is_leaf), entries=entries, M=self.M)


def from_point(x: float, y: float) -> MBR:
    return (x, x, y, y)

def rect_from_point_radius(x: float, y: float, r: float) -> MBR:
    return (x - r, x + r, y - r, y + r)

def intersects(a: MBR, b: MBR) -> bool:
    ax1, ax2, ay1, ay2 = a
    bx1, bx2, by1, by2 = b
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)

def area(m: MBR) -> float:
    x1, x2, y1, y2 = m
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)

def expand(a: MBR, b: MBR) -> MBR:
    ax1, ax2, ay1, ay2 = a
    bx1, bx2, by1, by2 = b
    return (min(ax1, bx1), max(ax2, bx2), min(ay1, by1), max(ay2, by2))

def enlargement(a: MBR, b: MBR) -> float:
    return area(expand(a, b)) - area(a)

def mindist_point_mbr(px: float, py: float, m: MBR) -> float:
    x1, x2, y1, y2 = m
    dx = (x1 - px) if px < x1 else (px - x2) if px > x2 else 0.0
    dy = (y1 - py) if py < y1 else (py - y2) if py > y2 else 0.0
    return dx * dx + dy * dy  # distancia^2 (evitamos sqrt)

def avg_fill(nodes_count: int, total_entries: int, M: int) -> float:
    if nodes_count == 0: return 0.0
    return total_entries / (nodes_count * M)

def basic_stats(height: int, nodes: int, pages_r: int, pages_w: int, fill: float) -> dict:
    return {
        "height": height,
        "nodes": nodes,
        "read_count": pages_r,
        "write_count": pages_w,
        "avg_fill": round(fill, 3)
    }


from pathlib import Path

class RTree:
    """
    Envoltorio delgado que:
    - Resuelve el path del archivo índice en runtime/files/<tabla>/<tabla>-rtree-<col>.idx
    - Pasa 'heap' y 'key' a tu RTreeFile sin que tengas que modificar su API
    - Expone métodos estándar: insert, search_rect, range, knn
    """
    def __init__(self, table: str, column: str, data_dir, *,
                 key: str = None, M: int = 32, heap_file: str = None):
        self.table = table
        self.column = column
        self.key = key or column
        self.heap_file = heap_file
        idx_dir = Path(data_dir) / table
        idx_dir.mkdir(parents=True, exist_ok=True)
        self.filename = str(idx_dir / f"{table}_rtree_{column}.idx")
        self.rt = RTreeFile(self.filename, M=M)

        # contadores como atributos (se actualizarán con _sync_io_counts)
        self.read_count = self.rt.store.read_count
        self.write_count = self.rt.store.write_count

        # Sidecar mapping for non-integer PK support: pk <-> int surrogate
        self.mapfile = str(idx_dir / f"{table}_rtree_{column}.map.json")
        self._p2i = {}
        self._i2p = {}
        self._next_id = 1
        self._load_map()

    # ---------- helpers ----------
    def _sync_io_counts(self):
        """Sincroniza los atributos públicos con los contadores del Storage."""
        self.read_count = self.rt.store.read_count
        self.write_count = self.rt.store.write_count

    # ---------- mapping helpers (non-int PK support) ----------
    def _load_map(self):
        import json
        try:
            if os.path.exists(self.mapfile):
                with open(self.mapfile, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._p2i = data.get("p2i", {}) or {}
                self._i2p = data.get("i2p", {}) or {}
                self._next_id = int(data.get("next_id", 1) or 1)
        except Exception:
            # tolerate corrupt map; start fresh (won't affect numeric PKs)
            self._p2i, self._i2p, self._next_id = {}, {}, 1

    def _save_map(self):
        import json
        try:
            with open(self.mapfile, "w", encoding="utf-8") as f:
                json.dump({
                    "next_id": self._next_id,
                    "p2i": self._p2i,
                    "i2p": self._i2p,
                }, f, ensure_ascii=False)
        except Exception:
            pass

    def _pk_key(self, pk):
        # Stable JSON string key for dicts; for scalars, str() is fine but we use json to handle types
        import json
        try:
            return json.dumps(pk, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(pk)

    def _pk_to_int(self, pk):
        # If already int-like, return as int
        try:
            if isinstance(pk, bool):
                # avoid True/False becoming 1/0 unintentionally for string PKs
                raise ValueError()
            if isinstance(pk, int):
                return int(pk)
        except Exception:
            pass
        k = self._pk_key(pk)
        if k in self._p2i:
            return int(self._p2i[k])
        # assign new surrogate id
        sid = int(self._next_id)
        self._next_id = sid + 1
        self._p2i[k] = sid
        self._i2p[str(sid)] = k
        self._save_map()
        return sid

    def _int_to_pk(self, sid):
        # If mapping exists, return original PK (deserialize JSON); else assume sid is the real PK (numeric)
        import json
        sk = str(int(sid))
        if sk in self._i2p:
            v = self._i2p[sk]
            try:
                return json.loads(v)
            except Exception:
                return v
        return int(sid)

    def close(self):
        """Cierra el archivo R-Tree subyacente (persiste el header con root/height actualizado)."""
        if DEBUG_IDX:
            print(f"[RTree.close] filename={self.filename} root={self.rt.store.root} height={self.rt.store.height} opened={self.rt.opened}")
        self.rt.close()
        self._sync_io_counts()
        if DEBUG_IDX:
            print(f"[RTree.close] after rt.close() opened={self.rt.opened} read={self.read_count} write={self.write_count}")

    def __del__(self):
        """Finalizer: Cierra el RTreeFile al destruir el wrapper (CPython: cierre al salir de scope)."""
        try:
            if DEBUG_IDX:
                print(f"[RTree.__del__] filename={self.filename}")
            self.close()
        except Exception:
            pass

    # --- escritura ---
    def insert(self, record: dict):
        # 'record' puede incluir 'pos' (heap) o 'pk' (no-heap)
        # Si 'pk' es no entero, usamos un surrogate int y lo almacenamos en el árbol
        additional = {"key": self.key, "heap": self.heap_file}
        rec = dict(record)
        if "pk" in rec and rec["pk"] is not None and not isinstance(rec["pk"], int):
            rec["pk"] = self._pk_to_int(rec["pk"])  # map non-int pk to surrogate
        res = self.rt.insert(rec, additional)
        self._sync_io_counts()
        return res

    def remove(self, record: dict):
        """
        Borra una entrada del R-Tree.
        Espera:
          - record["pos"] (obligatorio), record.get("slot", 0) (opcional)
          - record[self.key] opcional: [x,y] o [xmin,xmax,ymin,ymax] para verificar MBR exacto
        Devuelve: 1 si borró algo, 0 si no encontró.
        """
        rid = (record.get("pos"), record.get("slot", 0))
        if rid[0] is None:
            raise ValueError("RTree.remove: falta 'pos' en record")

        mbr = None
        if self.key in record:
            val = record[self.key]
            if isinstance(val, (list, tuple)) and len(val) == 2:
                x, y = map(float, val)
                mbr = from_point(x, y)
            elif isinstance(val, (list, tuple)) and len(val) == 4:
                mbr = tuple(map(float, val))

        add = {"rid": rid}
        if mbr is not None:
            add["mbr"] = mbr
        # heap no es necesario para borrar, pero no estorba
        if self.heap_file:
            add["heap"] = self.heap_file

        res = self.rt.remove(add)
        self._sync_io_counts()
        return res

    # --- lecturas ---
    def search_rect(self, xmin: float, xmax: float, ymin: float, ymax: float):
        items = self.rt.search({"rect": (xmin, xmax, ymin, ymax), "heap": self.heap_file})
        # Map back surrogate ids to original PKs when not using heap
        if not self.heap_file:
            out = []
            for it in (items or []):
                if isinstance(it, dict) and "pos" in it and isinstance(it.get("pos"), int):
                    it = dict(it)
                    it["pos"] = self._int_to_pk(it["pos"])  # may return int (original) or non-int
                out.append(it)
            self._sync_io_counts()
            return out
        self._sync_io_counts()
        return items

    def range(self, x: float, y: float, r: float):
        items = self.rt.range_search({"point": (x, y), "r": r, "heap": self.heap_file})
        if not self.heap_file:
            out = []
            for it in (items or []):
                if isinstance(it, dict) and "pos" in it and isinstance(it.get("pos"), int):
                    it = dict(it)
                    it["pos"] = self._int_to_pk(it["pos"])  # reverse map
                out.append(it)
            self._sync_io_counts()
            return out
        self._sync_io_counts()
        return items

    def knn(self, x: float, y: float, k: int):
        items = self.rt.knn({"point": (x, y), "k": int(k), "heap": self.heap_file})
        if not self.heap_file:
            out = []
            for it in (items or []):
                if isinstance(it, dict) and "pos" in it and isinstance(it.get("pos"), int):
                    it = dict(it)
                    it["pos"] = self._int_to_pk(it["pos"])  # reverse map
                out.append(it)
            self._sync_io_counts()
            return out
        self._sync_io_counts()
        return items
