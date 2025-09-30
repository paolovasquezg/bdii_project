# indexes/rtree/node.py
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from .mbr import MBR, expand

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
