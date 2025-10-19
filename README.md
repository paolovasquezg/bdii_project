### ⚙️ Implementación en el proyecto
El módulo del **R-Tree** se encuentra dentro de la carpeta:

```
indexes/rtree/
│── RTree.py
│── node.py
│── mbr.py
│── metrics.py
│── storage.py
```

Cada archivo cumple un rol específico:

| Archivo | Descripción |
|----------|-------------|
| **RTree.py** | Clase principal `RTree`: gestiona la raíz, inserciones, divisiones (*splits*), y búsquedas (por rango y k-NN). |
| **node.py** | Clase `Node`: representa los nodos del árbol (internos o hojas) y almacena referencias a los MBRs hijos. |
| **mbr.py** | Clase `MBR`: define las operaciones geométricas (intersección, unión, expansión y cálculo de área). |
| **metrics.py** | Contiene funciones para medir distancias y determinar el mejor nodo donde insertar un nuevo elemento. |
| **storage.py** | Gestiona la persistencia de los nodos en disco, simulando la estructura de almacenamiento físico. |

El índice se integra con el motor del proyecto mediante los módulos `File.py` y `TableCreate.py`, que reconocen el tipo de índice declarado en la creación de tablas.  
Ejemplo:

```sql
CREATE TABLE Restaurantes (
    id INT KEY INDEX SEQ,
    nombre VARCHAR[20] INDEX BTree,
    ubicacion ARRAY[FLOAT] INDEX RTree
);
```

---

### 🔍 Algoritmos principales implementados 

#### a. Inserción (`add`)
1. Se calcula el **MBR** del nuevo elemento.  
2. Se selecciona el nodo cuyo MBR necesite **menor aumento de área** (*enlargement mínimo*).  
3. Si el nodo se desborda, se ejecuta un **split cuadrático** para dividir el conjunto de entradas.  
4. Se actualizan los MBR de los nodos ancestros hasta la raíz.

**Complejidad:**  
- Búsqueda de nodo adecuado: `O(log_M n)`  
- Inserción y actualización de MBR: `O(1)`  
- Split (en caso de overflow): `O(M²)`  
> Complejidad promedio total: **O(log_M n)**  
> Complejidad en el peor caso: **O(M²)** (por splits repetidos).

---

#### b. Búsqueda por rango (`rangeSearch(point, radius)`)
1. Se genera un **MBR de consulta** a partir del punto y el radio.  
2. Se recorren todos los nodos cuyos MBR **intersecten** el MBR de búsqueda.  
3. En nodos hoja, se devuelven los objetos dentro del rango.

**Complejidad:**  
- Promedio: `O(log_M n + k)`  
- Peor caso: `O(n)` (si todos los MBR se solapan o cubren todo el espacio).

---

#### c. Búsqueda de vecinos cercanos (`rangeSearch(point, k)`)
1. Se calcula la distancia del punto de consulta a cada MBR.  
2. Se usa una **cola de prioridad** (min-heap) para expandir los nodos más cercanos primero.  
3. Se devuelven los **k elementos** más próximos.

**Complejidad:**  
- Construcción del heap: `O(n log k)`  
- Promedio (espacialmente balanceado): `O(log_M n + k log k)`.

---

### ⚖️ Tabla comparativa de métodos y complejidades

| Operación | Descripción | Complejidad promedio | Complejidad peor caso |
|------------|--------------|----------------------|-----------------------|
| **add()** | Inserta un nuevo registro en el árbol, actualizando los MBR. | O(log_M n) | O(M²) |
| **rangeSearch(point, radius)** | Devuelve los objetos dentro de un rango espacial. | O(log_M n + k) | O(n) |
| **rangeSearch(point, k)** | Devuelve los k elementos más cercanos al punto. | O(log_M n + k log k) | O(n log k) |
| **split()** | Divide un nodo lleno en dos, minimizando solapamiento. | O(M²) | O(M²) |
| **updateMBR()** | Ajusta los MBR ascendentes tras una inserción. | O(log_M n) | O(log_M n) |
