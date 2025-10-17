from backend.core.utils import build_format
from backend.catalog.catalog import get_json
from backend.core.record import Record
import struct

Order = 4

class Node:

    def __init__(self, order=Order, is_leaf=True, records=[], children=[], next_node=-1, parent=-1):
        self.order = order
        self.is_leaf = is_leaf
        self.records = records
        self.children = children
        self.next_node = next_node
        self.parent = parent
    
    def pack(self, recordSize):
        order_n_leaf = struct.pack("i?", self.order, self.is_leaf)
        num_recs = struct.pack("i", len(self.records))

        record_data = b''
        for record in self.records:
            record_data += record.pack()

        i = len(self.records)
        while i < Order:
            record_data += b'\x00' * recordSize
            i += 1

        num_child = struct.pack("i", len(self.children))

        child_data = b''
        for child in self.children:
            child_data += struct.pack("i", child)

        i = len(self.children)
        while i < Order + 1:
            child_data += struct.pack("i", 0)
            i += 1

        next_n_parent = struct.pack("ii", self.next_node, self.parent)

        return order_n_leaf + num_recs + record_data + num_child + child_data + next_n_parent

    @staticmethod
    def unpack(data: bytes, recordSize, formato, schema):
        header_fmt = "i?"
        header_size = struct.calcsize(header_fmt)
        order, is_leaf = struct.unpack(header_fmt, data[0:header_size])

        offset = header_size

        num_recs = struct.unpack("i", data[offset:offset+4])[0]
        offset += 4

        records = []
        record_area_size = Order * recordSize
        record_area = data[offset:offset+record_area_size]
        offset += record_area_size

        for i in range(num_recs):
            start = i * recordSize
            rec_bytes = record_area[start:start+recordSize]
            rec = Record.unpack(rec_bytes, formato, schema)
            records.append(rec)

        num_child = struct.unpack("i", data[offset:offset+4])[0]
        offset += 4

        children = []
        child_area_size = (Order + 1) * struct.calcsize("i")
        child_area = data[offset:offset+child_area_size]
        offset += child_area_size

        for i in range(num_child):
            start = i * struct.calcsize("i")
            child = struct.unpack("i", child_area[start:start+4])[0]
            children.append(child)

        next_node, parent = struct.unpack("ii", data[offset:offset+8])

        node = Node(order=order, is_leaf=is_leaf, records=records, children=children, next_node=next_node, parent=parent)
        return node
        

class BPlusFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.schema = get_json(self.filename)[0] 
        self.format = build_format(self.schema)
        self.REC_SIZE = struct.calcsize(self.format)
        self.NODE_HEADER_FMT = "i?"
        self.NODE_HEADER_SIZE = struct.calcsize(self.NODE_HEADER_FMT)
        self.INT_SIZE = struct.calcsize("i")
        self.PAGE_SIZE = (self.NODE_HEADER_SIZE + 4 + (Order * self.REC_SIZE) + 4 + ((Order + 1) * self.INT_SIZE) + 8)
        self.read_count = 0
        self.write_count = 0
        self._ensure_root()


    def _inc_read(self, n: int = 1):
        self.read_count += n

    def _inc_write(self, n: int = 1):
        self.write_count += n

    def _read_schema_size(self, f):
        f.seek(0)
        self._inc_read()
        return struct.unpack('I', f.read(4))[0]

    def _page_offset(self, schema_size, page: int):
        return 4 + schema_size + (page - 1) * self.PAGE_SIZE

    def _total_pages(self, f, schema_size):
        f.seek(0, 2)
        end = f.tell()
        data_region = end - (4 + schema_size)
        if data_region <= 0:
            return 0
        return data_region // self.PAGE_SIZE

    def _read_node_at(self, f, schema_size, page: int):
        off = self._page_offset(schema_size, page)
        f.seek(off)
        data = f.read(self.PAGE_SIZE)
        self._inc_read()
        return Node.unpack(data, self.REC_SIZE, self.format, self.schema)

    def _write_node_at(self, f, schema_size, page: int, node: Node):
        self._inc_write()
        off = self._page_offset(schema_size, page)
        f.seek(off)
        data = node.pack(self.REC_SIZE)
        if len(data) < self.PAGE_SIZE:
            data += b'\x00' * (self.PAGE_SIZE - len(data))
        f.write(data)

    def _append_node(self, f, schema_size, node: Node):
        total = self._total_pages(f, schema_size)
        new_page = total + 1
        off = self._page_offset(schema_size, new_page)
        f.seek(off)
        data = node.pack(self.REC_SIZE)
        if len(data) < self.PAGE_SIZE:
            data += b'\x00' * (self.PAGE_SIZE - len(data))
        f.write(data)
        self._inc_write(len(node.records))
        return new_page

    def _ensure_root(self):
        with open(self.filename, 'r+b') as f:
            schema_size = self._read_schema_size(f)
            total = self._total_pages(f, schema_size)
            if total == 0:
                root = Node(is_leaf=True, records=[], children=[], next_node=-1, parent=-1)
                self._append_node(f, schema_size, root)

    def _get_root_page(self):
        return 1

    def _get_key_from_record(self, rec: Record, keyname: str):
        return rec.fields.get(keyname)

    def _find_leaf_page(self, value, keyname):
        with open(self.filename, 'rb') as f:
            schema_size = self._read_schema_size(f)
            page = self._get_root_page()
            while True:
                node = self._read_node_at(f, schema_size, page)
                if node.is_leaf:
                    return page, node
                i = 0
                while i < len(node.records) and value > self._get_key_from_record(node.records[i], keyname):
                    i += 1
                if i < len(node.children):
                    page = node.children[i]
                else:
                    page = node.children[-1]

    def insert(self, record: dict, additional: dict):
        keyname = additional['key']
        record['deleted'] = False
        with open(self.filename, 'r+b') as f:
            schema_size = self._read_schema_size(f)
            leaf_page, leaf = self._find_leaf_page(record[keyname], keyname)

            for r in leaf.records:
                if not r.fields.get('deleted') and r.fields.get(keyname) == record[keyname]:
                    if additional.get('unique'):
                        return []

            new_rec = Record(self.schema, self.format, record)
            i = 0
            while i < len(leaf.records) and self._get_key_from_record(leaf.records[i], keyname) < record[keyname]:
                i += 1

            leaf.records.insert(i, new_rec)

            max_keys = Order - 1
            if len(leaf.records) > max_keys:
                self._split_leaf(f, schema_size, leaf_page, leaf, additional)
            else:
                self._write_node_at(f, schema_size, leaf_page, leaf)

        return [new_rec.fields]

    def _split_leaf(self, f, schema_size, leaf_page, leaf_node, additional):
        keyname = additional['key']
        total = self._total_pages(f, schema_size)

        mid = (len(leaf_node.records) + 1) // 2
        right = Node(is_leaf=True, records=leaf_node.records[mid:], children=[], next_node=leaf_node.next_node, parent=leaf_node.parent)
        left_records = leaf_node.records[:mid]
        leaf_node.records = left_records

        right_page = self._append_node(f, schema_size, right)

        leaf_node.next_node = right_page
        self._write_node_at(f, schema_size, leaf_page, leaf_node)

        promote_key = self._get_key_from_record(right.records[0], keyname)

        self._insert_in_parent(f, schema_size, leaf_page, promote_key, right_page, additional)

    def _insert_in_parent(self, f, schema_size, left_page, key_value, right_page, additional):
        left_node = self._read_node_at(f, schema_size, left_page)
        parent_page = left_node.parent
        keyname = additional['key']

        promote_record = Record(self.schema, self.format, {keyname: key_value})

        if parent_page == -1:
            total = self._total_pages(f, schema_size)
            if left_page == 1:
                old_root = left_node
                old_root.parent = -1
                new_left_page = self._append_node(f, schema_size, old_root)
        
                new_root = Node(is_leaf=False, records=[promote_record], children=[new_left_page, right_page], next_node=-1, parent=-1)
                left_moved = self._read_node_at(f, schema_size, new_left_page)
                left_moved.parent = 1
                self._write_node_at(f, schema_size, new_left_page, left_moved)
                right_moved = self._read_node_at(f, schema_size, right_page)
                right_moved.parent = 1
                
                self._write_node_at(f, schema_size, right_page, right_moved)

                self._write_node_at(f, schema_size, 1, new_root)
            else:
                new_root = Node(is_leaf=False, records=[promote_record], children=[left_page, right_page], next_node=-1, parent=-1)
                appended = self._append_node(f, schema_size, new_root)
                new_root.parent = -1
                child_left = self._read_node_at(f, schema_size, left_page)
                child_left.parent = 1
                self._write_node_at(f, schema_size, left_page, child_left)
                child_right = self._read_node_at(f, schema_size, right_page)
                child_right.parent = 1
                self._write_node_at(f, schema_size, right_page, child_right)
                root_node = new_root
                self._write_node_at(f, schema_size, 1, root_node)
        else:
            parent = self._read_node_at(f, schema_size, parent_page)
            try:
                idx = parent.children.index(left_page)
            except ValueError:
                idx = 0
                while idx < len(parent.records) and key_value >= self._get_key_from_record(parent.records[idx], keyname):
                    idx += 1

            parent.records.insert(idx, promote_record)
            parent.children.insert(idx + 1, right_page)

            right_node = self._read_node_at(f, schema_size, right_page)
            right_node.parent = parent_page
            self._write_node_at(f, schema_size, right_page, right_node)

            if len(parent.children) > Order:
                self._write_node_at(f, schema_size, parent_page, parent)
                self._split_internal(f, schema_size, parent_page, parent, additional)
            else:
                self._write_node_at(f, schema_size, parent_page, parent)

    def _split_internal(self, f, schema_size, node_page, node, additional):
        keyname = additional['key']
        mid = len(node.records) // 2
        promote_rec = node.records[mid]
        promote_key = self._get_key_from_record(promote_rec, keyname)

        right = Node(is_leaf=False, records=node.records[mid + 1:], children=node.children[mid + 1:], next_node=-1, parent=node.parent)
        for child_page in right.children:
            child = self._read_node_at(f, schema_size, child_page)
            child.parent = None 
            child.parent = None
            child.parent = None
        node.records = node.records[:mid]
        node.children = node.children[:mid + 1]

        self._write_node_at(f, schema_size, node_page, node)

        right_page = self._append_node(f, schema_size, right)

        for child_page in right.children:
            child = self._read_node_at(f, schema_size, child_page)
            child.parent = right_page
            self._write_node_at(f, schema_size, child_page, child)

        self._insert_in_parent(f, schema_size, node_page, promote_key, right_page, additional)

    def search(self, additional: dict, same_key: bool = True):
        keyname = additional['key']
        val = additional['value']
        results = []
        with open(self.filename, 'rb') as f:
            schema_size = self._read_schema_size(f)

            if not same_key:
                root_page = self._get_root_page()
                page = root_page
                while True:
                    node = self._read_node_at(f, schema_size, page)
                    if node.is_leaf:
                        break
                    if len(node.children) > 0:
                        page = node.children[0]
                    else:
                        break

                while page != -1:
                    node = self._read_node_at(f, schema_size, page)
                    for rec in node.records:
                        if rec.fields.get(keyname) == val and not rec.fields.get('deleted'):
                            del rec.fields['deleted']
                            results.append(rec.fields)
                            if additional.get('unique'):
                                return results
                    page = node.next_node

                return results

            leaf_page, leaf = self._find_leaf_page(val, keyname)
            leftmost_page = leaf_page
            current = leaf_page
            while True:
                node = self._read_node_at(f, schema_size, current)
                if node.parent == -1:
                    break

                parent = self._read_node_at(f, schema_size, node.parent)
                try:
                    idx = parent.children.index(current)
                except ValueError:
                    break

                if idx == 0:
                    break

                left_sibling_page = parent.children[idx - 1]
                left_sibling = self._read_node_at(f, schema_size, left_sibling_page)

                if not left_sibling.records:
                    break

                last_key = self._get_key_from_record(left_sibling.records[-1], keyname)
                if last_key < val:
                    break

                leftmost_page = left_sibling_page
                current = left_sibling_page

            page = leftmost_page
            while page != -1:
                node = self._read_node_at(f, schema_size, page)
                found_any = False
                for rec in node.records:
                    rec_key = self._get_key_from_record(rec, keyname)
                    if rec_key > val:
                        # Past our target value, stop scanning
                        return results
                    if rec_key == val and not rec.fields.get('deleted'):
                        del rec.fields['deleted']
                        results.append(rec.fields)
                        found_any = True
                        if additional.get('unique'):
                            return results

                if node.records and not found_any:
                    last_key = self._get_key_from_record(node.records[-1], keyname)
                    if last_key < val:
                        page = node.next_node
                        continue
                    if last_key > val:
                        break

                page = node.next_node

        return results

    def range_search(self, additional: dict, same_key: bool = True):
        keyname = additional['key']
        min_v = additional['min']
        max_v = additional['max']
        results = []
        with open(self.filename, 'rb') as f:
            schema_size = self._read_schema_size(f)

            if not same_key:
                root_page = self._get_root_page()
                page = root_page
                while True:
                    node = self._read_node_at(f, schema_size, page)
                    if node.is_leaf:
                        break
                    if len(node.children) > 0:
                        page = node.children[0]
                    else:
                        break

                while page != -1:
                    node = self._read_node_at(f, schema_size, page)
                    for rec in node.records:
                        val = rec.fields.get(keyname)
                        if val is None:
                            continue
                        if val > max_v:
                            return results
                        if min_v <= val <= max_v and not rec.fields.get('deleted'):
                            del rec.fields['deleted']
                            results.append(rec.fields)
                    page = node.next_node

                return results

            leaf_page, leaf = self._find_leaf_page(min_v, keyname)
            page = leaf_page
            while page != -1:
                node = self._read_node_at(f, schema_size, page)
                for rec in node.records:
                    val = rec.fields.get(keyname)
                    if val is None:
                        continue
                    if val > max_v:
                        return results
                    if min_v <= val <= max_v and not rec.fields.get('deleted'):
                        del rec.fields['deleted']
                        results.append(rec.fields)
                page = node.next_node

        return results

    def remove(self, additional: dict, same_key: bool = True):
        keyname = additional['key']
        val = additional['value']
        removed = []
        with open(self.filename, 'r+b') as f:
            schema_size = self._read_schema_size(f)

            if not same_key:
                root_page = self._get_root_page()
                page = root_page
                while True:
                    node = self._read_node_at(f, schema_size, page)
                    if node.is_leaf:
                        break
                    if len(node.children) > 0:
                        page = node.children[0]
                    else:
                        break

                while page != -1:
                    node = self._read_node_at(f, schema_size, page)
                    modified = False
                    for idx, rec in enumerate(node.records):
                        if rec.fields.get(keyname) == val and not rec.fields.get('deleted'):
                            rec.fields['deleted'] = True
                            node.records[idx] = rec
                            removed.append({k: v for k, v in rec.fields.items() if k != 'deleted'})
                            modified = True
                            if additional.get('unique'):
                                break
                    if modified:
                        self._write_node_at(f, schema_size, page, node)
                    if removed and additional.get('unique'):
                        break
                    page = node.next_node

                return removed

            leaf_page, leaf = self._find_leaf_page(val, keyname)
            page = leaf_page
            while page != -1:
                node = self._read_node_at(f, schema_size, page)
                modified = False
                for idx, rec in enumerate(node.records):
                    if rec.fields.get(keyname) == val and not rec.fields.get('deleted'):
                        rec.fields['deleted'] = True
                        node.records[idx] = rec
                        removed.append({k: v for k, v in rec.fields.items() if k != 'deleted'})
                        modified = True
                        if additional.get('unique'):
                            break
                if modified:
                    self._write_node_at(f, schema_size, page, node)
                if removed and additional.get('unique'):
                    break
                page = node.next_node

        return removed