# -*- coding: utf-8 -*-
"""
test_utils.py — utilidades para E2E:
- Engine runner sin imprimir JSON por defecto
- Asserts consistentes con el "sobre" (envelope) URE
- Helpers de dataset CSV y conteos
"""
import os, sys, json, csv

HERE = os.path.abspath(os.path.dirname(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Engine import (ambos layouts)
try:
    from backend.engine.engine import Engine
except Exception:
    from backend.engine import Engine  # type: ignore

ENGINE = Engine()

def run_sql(sql: str, *, print_json: bool = False) -> dict:
    """Ejecuta SQL y retorna el envelope. No imprime JSON salvo que print_json=True."""
    env = ENGINE.run(sql)
    if print_json:
        print(json.dumps(env, indent=2, ensure_ascii=False))
    return env

def assert_env_ok(env: dict, *, msg: str = "") -> dict:
    """Exige env.ok=True y results[0].ok=True; devuelve el primer result."""
    if not isinstance(env, dict) or not env.get("ok", False):
        raise AssertionError(f"Envelope NOT ok{': '+msg if msg else ''}: {env}")
    results = env.get("results", [])
    if not results or not isinstance(results[0], dict) or not results[0].get("ok", False):
        raise AssertionError(f"Result NOT ok{': '+msg if msg else ''}: {env}")
    return results[0]

def expect_error(sql: str) -> dict:
    """
    Ejecuta SQL esperando error (env.ok=False o results[0].ok=False).
    No imprime JSON (evita tripear al runner).
    """
    env = run_sql(sql, print_json=False)
    ok = env.get("ok", False)
    r0 = (env.get("results") or [{}])[0]
    rok = r0.get("ok", False)
    if ok and rok:
        raise AssertionError(f"Se esperaba error pero fue OK: {env}")
    # Retornamos el envelope por si el test quiere inspeccionar meta/error
    return env

def get_rows(res: dict) -> list:
    """Safe rows desde un result ya-ok."""
    return list(res.get("data") or [])

def ensure_products_csv(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id","name","price","stock"])
        w.writerows([
            [1,"Alpha",10.5,50],
            [2,"Bravo",20.0,15],
            [3,"Charlie",15.0,30],
            [4,"Delta",25.0,5],
            [5,"Echo",30.5,0],
        ])

def ok(msg: str): print(f"  ✓ {msg}")
def bad(msg: str): raise AssertionError(msg)
