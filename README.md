## 🧱 Técnica de Indexación: R-Tree

### 📌 1. Introducción al índice R-Tree
El **R-Tree (Rectangle Tree)** es una estructura jerárquica de indexación espacial diseñada para organizar **datos multidimensionales** (por ejemplo, coordenadas geográficas, polígonos o regiones).  
A diferencia de los índices tradicionales basados en valores escalares (como el B+Tree o el ISAM), el R-Tree agrupa **objetos por su localización y extensión en el espacio**, utilizando **rectángulos mínimos contenedores** (*MBR — Minimum Bounding Rectangles*).

Cada nodo del árbol representa un conjunto de objetos o subregiones del espacio. De esta manera, el R-Tree permite ejecutar operaciones de búsqueda **por rango** y **por proximidad** sin explorar todos los registros, reduciendo el número de accesos a disco.

---

### ⚙️ 2. Implementación en el proyecto
El módulo del R-Tree se encuentra dentro de la carpeta:

```
indexes/rtree/
│── RTree.py
│── node.py
│── mbr.py
│── metrics.py
│── storage.py
```

Cada archivo cumple un rol específico:

| Archivo | Función principal |
|----------|------------------|
| **RTree.py** | Implementa la clase `RTree`, que gestiona la raíz, las inserciones y las búsquedas por rango y k-NN. Controla las divisiones (*splits*) y el crecimiento del árbol. |
| **node.py** | Define la clase `Node`, que representa un nodo del árbol (hoja o interno). Mantiene las entradas y los límites de cada MBR. |
| **mbr.py** | Define la clase `MBR`, encargada de los cálculos geométricos: intersecciones, uniones y área de expansión. |
| **metrics.py** | Contiene funciones para medir distancia euclidiana y determinar el MBR más adecuado para inserciones. |
| **storage.py** | Administra la serialización de los nodos en disco, simulando la persistencia del índice espacial. |

El R-Tree se integra al mini-gestor mediante los módulos `File.py` y `TableCreate.py`, que reconocen el tipo de índice especificado en el esquema de la tabla.

Ejemplo de uso en SQL:
```sql
CREATE TABLE Restaurantes (
    id INT KEY INDEX SEQ,
    nombre VARCHAR[20] INDEX BTree,
    ubicacion ARRAY[FLOAT] INDEX RTree
);
```

---

### 🔍 3. Algoritmos principales implementados

#### a. Inserción (`add`)
1. Se calcula el **MBR** del nuevo objeto (por ejemplo, una coordenada o un rectángulo).  
2. Se selecciona recursivamente el nodo cuyo MBR **requiera menor aumento de área** para incluir el nuevo objeto (*mínimo enlargement*).  
3. Si el nodo se desborda (supera su capacidad `M`), se aplica un **split cuadrático**, dividiendo las entradas en dos nodos con solapamiento mínimo.  
4. Se actualizan los MBR de todos los nodos ascendentes hasta la raíz.

#### b. Búsqueda por rango (`rangeSearch(point, radius)`)
1. Se construye un **MBR de búsqueda** a partir del punto y el radio dado.  
2. Se recorren los nodos cuyos MBR **intersecten** el MBR de búsqueda.  
3. En los nodos hoja, se devuelven todos los objetos contenidos o parcialmente contenidos en esa región.

#### c. Búsqueda de vecinos cercanos (`rangeSearch(point, k)`)
1. Se calcula la distancia del punto de consulta a cada MBR.  
2. Se mantiene una cola prioritaria ordenada por distancia.  
3. Se recorren los nodos más prometedores primero, devolviendo los **k registros más próximos**.

---

### 🧠 4. Validación y pruebas (“de esquina a esquina”)
Durante la fase de pruebas (`Testing.py`), se validó el correcto funcionamiento del R-Tree con consultas **de esquina a esquina**, es decir, búsquedas donde el MBR de consulta abarca todo el espacio (desde las coordenadas mínimas hasta las máximas).

Este tipo de prueba garantiza que:
- La función `intersects()` de los MBR detecta correctamente objetos **ubicados en los bordes**.  
- El recorrido del árbol es completo y no omite regiones del espacio.  
- La estructura mantiene su integridad tras múltiples inserciones y splits.

Ejemplo de prueba:
```python
query_mbr = MBR(0, 0, 100, 100)
resultados = rtree.range_search(query_mbr)
```

Resultado esperado: el método devuelve **todos los puntos** del dominio, confirmando que los límites espaciales están correctamente manejados.

---

### ⚖️ 5. Comparación teórica
| Técnica | Tipo de datos | Ventaja principal | Limitación |
|----------|---------------|------------------|-------------|
| **Sequential / ISAM / B+Tree** | Unidimensional (números, texto) | Accesos rápidos para rangos ordenados | No manejan relaciones espaciales |
| **R-Tree** | Multidimensional (coordenadas, regiones) | Agrupación espacial, consultas por intersección y k-NN | Solapamiento entre nodos puede degradar rendimiento |

El R-Tree es, por tanto, la técnica más adecuada para **datos espaciales** dentro del sistema, complementando a los otros índices tradicionales que gestionan datos escalares.

---

### 🧩 6. Conclusión
El módulo **R-Tree** implementado proporciona un mecanismo eficiente para indexar y consultar datos espaciales dentro del mini-gestor de base de datos.  
Su integración con el motor de tablas permite realizar operaciones de búsqueda por rango y vecinos cercanos sobre campos multidimensionales, cumpliendo con los objetivos del Proyecto 1.  
Además, esta base servirá para futuras extensiones del sistema hacia la **indexación geoespacial y multimodal** en la segunda fase del curso.
