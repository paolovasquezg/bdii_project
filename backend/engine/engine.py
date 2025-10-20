from time import perf_counter
from dataclasses import asdict, is_dataclass
from backend.sql.parser import SQLParser
from backend.planner.planner import Planner
from backend.engine.executor import Executor, err_result   # <-- importa err_result


def _sum_results_time_ms(results) -> float:
    """
    Suma defensiva de tiempos por resultado. Usa 'meta.time_ms' o 'stats.time_ms' si existe.
    Si algún resultado viniera con tiempos acumulados (legacy), evita sobrecontar.
    """
    times = []
    if not isinstance(results, list):
        return 0.0
    for r in results:
        if not isinstance(r, dict):
            continue
        for bucket in ("meta", "stats"):
            b = r.get(bucket)
            if isinstance(b, dict) and isinstance(b.get("time_ms"), (int, float)):
                times.append(float(b["time_ms"]))
                break
    if not times:
        return 0.0
    # Normaliza por si vinieron acumulados: ordena y resta el previo
    times.sort()
    total, prev = 0.0, 0.0
    for t in times:
        total += max(0.0, t - prev)
        prev = max(prev, t)
    return total

class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.planner = Planner()
        self.exec = Executor()


    def run(self, sql: str):
        t0 = perf_counter()

        # 1) Parseo
        try:
            ast = self.parser.parse(sql)
        except Exception as ex:
            total_ms = (perf_counter() - t0) * 1000.0
            results = [err_result(
                action="parse",
                code="SYNTAX_ERROR",
                message=str(ex),
                where="parser",
                detail={"sql": sql}
            )]
            return {"ok": False, "schema": "bd2.v1", "results": results, "warnings": [], "stats": {"time_ms": total_ms}}

        t1 = perf_counter()

        # 2) Planificación
        try:
            plans = self.planner.plan(ast)
        except Exception as ex:
            total_ms = (perf_counter() - t0) * 1000.0
            safe_ast = []
            try:
                safe_ast = [asdict(x) if is_dataclass(x) else x for x in ast]
            except Exception:
                pass
            results = [err_result(
                action="plan",
                code="PLAN_ERROR",
                message=str(ex),
                where="planner",
                detail={"ast": safe_ast}
            )]
            return {"ok": False, "schema": "bd2.v1", "results": results, "warnings": [], "stats": {"time_ms": total_ms}}

        t2 = perf_counter()

        # 3) Ejecución
        env = self.exec.run(plans)

        t3 = perf_counter()

        # ---- tiempos ----
        parse_ms = (t1 - t0) * 1000.0
        plan_ms = (t2 - t1) * 1000.0
        exec_ms = (t3 - t2) * 1000.0

        stats = env.setdefault("stats", {})

        stats["parse_ms"] = parse_ms
        stats["plan_ms"] = plan_ms
        stats["exec_ms"] = exec_ms

        # ⬇️ clave: pisa cualquier valor previo y define el total del pipeline
        stats["time_ms"] = parse_ms + plan_ms + exec_ms

        return env
