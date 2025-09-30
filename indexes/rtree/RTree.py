from typing import List, Tuple
from indexes.Heap import HeapFile
from .node import Node, Entry
from .mbr import MBR, from_point, rect_from_point_radius, intersects, enlargement, mindist_point_mbr, expand
from .storage import Storage
from .metrics import basic_stats

class RTreeFile:
    def __init__(self, filename: str, M: int = 32):
        self.store = Storage(filename, M=M)
        self.opened = False

    # ---------- lifecycle ----------
    def open(self):
        if not self.opened:
            self.store.open()
            if self.store.root == 0:
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
        """record debe traer: 'pos', opcional 'slot', y la columna espacial en additional['key']"""
        self.open()
        key = additional["key"]        # ej. "ubicacion"
        val = record[key]
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
        pos = record["pos"]; slot = record.get("slot", 0)
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

    def remove(self, additional: dict):
        # opcional (no requerido por la mayoría de cursos)
        return 0

    def stats(self) -> dict:
        return basic_stats(self.store.height, -1, self.store.pages_read, self.store.pages_written, 0.0)
