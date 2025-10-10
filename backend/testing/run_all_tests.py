#!/usr/bin/env python3
"""
run_all_tests.py
----------------
Ejecuta en secuencia estos tests "smoke" del proyecto BD2:

  - parser_test.py
  - heap_test.py
  - seq_test.py
  - isam_test.py
  - rtree_test.py

Características:
- Corre cada script como subproceso (aislado), muestra su salida en vivo y
  captura el código de retorno (0=OK, distinto de 0=FAIL).
- Permite elegir subconjuntos con --only y detener en el primer fallo con
  --stop-on-fail.
- Busca automáticamente los tests en:
    1) El mismo directorio donde está este runner
    2) backend/testing/ (si existe)
- Muestra un resumen final (PASSED/FAILED) con tiempos.

Uso:
  python run_all_tests.py                 # corre todos
  python run_all_tests.py --only parser,isam
  python run_all_tests.py --stop-on-fail

Requisitos:
- Python 3.8+
- El proyecto debe poder importar backend.engine.engine.Engine desde la CWD
  o desde el propio test (los tests que subiste ya lo manejan).
"""

import argparse
import os
import sys
import time
import subprocess
from pathlib import Path
from typing import List, Tuple

# ---------- Config ----------
DEFAULT_ORDER = [
    "parser_test.py",
    "heap_test.py",
    "seq_test.py",
    "isam_test.py",
    "rtree_test.py",
]

SEARCH_DIRS = [
    Path(__file__).parent,                   # junto al runner
    Path.cwd(),                              # CWD
    Path("backend") / "testing",             # típico en el repo
]

PYTHON = sys.executable or "python3"


def find_test_file(name: str) -> Path:
    """Devuelve la ruta del test si existe en alguno de los SEARCH_DIRS."""
    for base in SEARCH_DIRS:
        p = (base / name).resolve()
        if p.exists():
            return p
    # último intento: por si pasaron ruta directa
    p = Path(name).resolve()
    if p.exists():
        return p
    return None


def run_test(path: Path) -> Tuple[int, float]:
    """Ejecuta un test (script .py) en un subproceso, stream de salida, devuelve (returncode, seconds)."""
    print("\n" + "=" * 80)
    print(f"▶ RUNNING: {path}")
    print("=" * 80)
    sys.stdout.flush()

    start = time.time()
    # Ejecutamos en el directorio del test para respetar rutas relativas internas
    cwd = path.parent
    proc = subprocess.Popen(
        [PYTHON, str(path.name)],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )
    # Stream de salida en vivo
    for line in proc.stdout:
        print(line, end="")
    proc.wait()
    elapsed = time.time() - start

    rc = proc.returncode
    status = "OK" if rc == 0 else f"FAIL (rc={rc})"
    print("-" * 80)
    print(f"↳ RESULT: {status}  •  time={elapsed:.2f}s  •  test={path.name}")
    print("-" * 80)
    return rc, elapsed


def main():
    ap = argparse.ArgumentParser(description="Runner de tests BD2")
    ap.add_argument("--only", help="Lista separada por comas: parser,heap,seq,isam,rtree", default="")
    ap.add_argument("--stop-on-fail", action="store_true", help="Detener en el primer fallo")
    args = ap.parse_args()

    # Mapeo simple de alias -> filename
    alias_map = {
        "parser": "parser_test.py",
        "heap": "heap_test.py",
        "seq": "seq_test.py",
        "isam": "isam_test.py",
        "rtree": "rtree_test.py",
    }

    order: List[str] = DEFAULT_ORDER[:]
    if args.only.strip():
        wanted = [a.strip().lower() for a in args.only.split(",") if a.strip()]
        order = []
        for w in wanted:
            order.append(alias_map.get(w, w))  # acepta alias o nombre de archivo

    # Resolver paths
    tests: List[Path] = []
    missing: List[str] = []
    for name in order:
        p = find_test_file(name)
        if p is None:
            missing.append(name)
        else:
            tests.append(p)

    if missing:
        print("⚠ No se encontraron estos tests:", ", ".join(missing))
        print("   Directorios buscados:", ", ".join(str(d.resolve()) for d in SEARCH_DIRS))

    if not tests:
        print("No hay tests para ejecutar. Saliendo.")
        sys.exit(1)

    print("Plan de ejecución:")
    for i, t in enumerate(tests, 1):
        print(f"  {i:>2}. {t}")
    print()

    # Ejecutar
    summary = []
    failures = 0
    total_time = 0.0

    for t in tests:
        rc, dt = run_test(t)
        total_time += dt
        summary.append((t.name, rc, dt))
        if rc != 0:
            failures += 1
            if args.stop_on_fail:
                print("\n⛔ Detenido por --stop-on-fail.")
                break

    # Resumen
    print("\n" + "#" * 80)
    print("RESUMEN")
    print("#" * 80)
    for name, rc, dt in summary:
        mark = "✅" if rc == 0 else "❌"
        print(f"{mark} {name:15s}  rc={rc}  time={dt:.2f}s")
    print("-" * 80)
    print(f"TOTAL: {len(summary)} tests  •  FAILS={failures}  •  time={total_time:.2f}s")
    print("#" * 80)

    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
        sys.exit(130)