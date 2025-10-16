import argparse
import os
import sys
import time
import subprocess
import re
from pathlib import Path
from typing import List, Tuple

# ---------- Config ----------
DEFAULT_ORDER = [
    "parser_test.py",
    "heap_test.py",
    "seq_test.py",
    "isam_test.py",
    "rtree_test.py",
    "bplus_test.py",
    "hash_test.py",
]

SEARCH_DIRS = [
    Path(__file__).parent,
    Path.cwd(),
    Path("backend") / "testing",
]

PYTHON = sys.executable or "python3"

# Detecta ok:false tanto en JSON como en repr de dicts de Python
RE_OK_FALSE = re.compile(r"""["']ok["']\s*:\s*(false|False)""")
RE_FAIL_TAG = re.compile(r"\[FAIL\]")

def find_test_file(name: str) -> Path | None:
    for base in SEARCH_DIRS:
        p = (base / name).resolve()
        if p.exists():
            return p
    p = Path(name).resolve()
    return p if p.exists() else None

def run_test(path: Path) -> Tuple[int, float, int]:
    """
    Ejecuta un test (script .py) en un subproceso, stream de salida,
    y devuelve (returncode_final, seconds, ok_false_count_detected).
    Si se detecta cualquier ok:false o [FAIL], se fuerza FAIL en el informe.
    """
    print("\n" + "=" * 80)
    print(f"▶ RUNNING: {path}")
    print("=" * 80)
    sys.stdout.flush()

    start = time.time()
    cwd = path.parent
    proc = subprocess.Popen(
        [PYTHON, str(path.name)],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )

    ok_false_hits = 0
    saw_fail_hint = False

    # Stream de salida en vivo + escaneo
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        if RE_OK_FALSE.search(line):
            ok_false_hits += 1
        if RE_FAIL_TAG.search(line):
            saw_fail_hint = True

    proc.wait()
    elapsed = time.time() - start

    rc = proc.returncode
    # Si el subproceso "pasó" pero vimos ok:false o [FAIL], marcamos FAIL
    if rc == 0 and (ok_false_hits > 0 or saw_fail_hint):
        rc = 1
        print("⚠ Nota: Se detectó al menos un ok:false o [FAIL] en la salida; "
              "se marca este test como FAIL para el informe.")
    status = "OK" if rc == 0 else f"FAIL (rc={rc})"
    print("-" * 80)
    print(f"↳ RESULT: {status}  •  time={elapsed:.2f}s  •  test={path.name}  •  ok:false={ok_false_hits}")
    print("-" * 80)
    return rc, elapsed, ok_false_hits

def main():
    ap = argparse.ArgumentParser(description="Runner de tests BD2 (estricto con ok:false)")
    ap.add_argument("--only", help="Lista separada por comas: parser,heap,seq,isam,rtree,bplus,hash", default="")
    ap.add_argument("--stop-on-fail", action="store_true", help="Detener en el primer fallo")
    args = ap.parse_args()

    alias_map = {
        "parser": "parser_test.py",
        "heap": "heap_test.py",
        "seq": "seq_test.py",
        "isam": "isam_test.py",
        "rtree": "rtree_test.py",
        "bplus": "bplus_test.py",
        "hash": "hash_test.py",
    }

    order: List[str] = DEFAULT_ORDER[:]
    if args.only.strip():
        wanted = [a.strip().lower() for a in args.only.split(",") if a.strip()]
        order = [alias_map.get(w, w) for w in wanted]

    tests: List[Path] = []
    missing: List[str] = []
    for name in order:
        p = find_test_file(name)
        (tests if p else missing).append(p or name)

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

    summary = []
    failures = 0
    total_time = 0.0

    for t in tests:
        rc, dt, hits = run_test(t)
        total_time += dt
        summary.append((t.name, rc, dt, hits))
        if rc != 0:
            failures += 1
            if args.stop_on_fail:
                print("\n⛔ Detenido por --stop-on-fail.")
                break

    # Resumen
    print("\n" + "#" * 80)
    print("RESUMEN")
    print("#" * 80)
    for name, rc, dt, hits in summary:
        mark = "✅" if rc == 0 else "❌"
        extra = f" • ok:false={hits}" if hits else ""
        print(f"{mark} {name:15s}  rc={rc}  time={dt:.2f}s{extra}")
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