from time import perf_counter
from dataclasses import asdict, is_dataclass
from backend.sql.parser import SQLParser
from backend.planner.planner import Planner
from backend.engine.executor import Executor, err_result   # <-- importa err_result


def _sum_results_time_ms(results) -> float:
    total = 0.0
    if not isinstance(results, list):
        return 0.0
    for r in results:
        if not isinstance(r, dict):
            continue
        for bucket in ("stats", "meta"):
            b = r.get(bucket)
            if isinstance(b, dict):
                v = b.get("time_ms")
                if isinstance(v, (int, float)):
                    total += float(v)
    return total


class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.planner = Planner()
        self.exec = Executor()

    def run(self, sql: str):
        t0 = perf_counter()

        # 1) Parseo: si falla, envelope con ok=false
        try:
            ast = self.parser.parse(sql)      # SQL -> AST
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

        # 2) Planificación: si falla, envelope con ok=false
        try:
            plans = self.planner.plan(ast)    # AST -> Planes
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

        # 3) Ejecución: Executor ya devuelve envelope (y captura errores internos)
        env = self.exec.run(plans)

        t3 = perf_counter()

        # ---- tiempos ----
        parse_ms = (t1 - t0) * 1000.0
        plan_ms  = (t2 - t1) * 1000.0
        exec_ms  = (t3 - t2) * 1000.0

        # Sumar time_ms de cada resultado (si existen dentro de stats/meta)
        results_ms_sum = _sum_results_time_ms(env.get("results", []))

        stats = env.setdefault("stats", {})

        stats.setdefault("exec_ms", exec_ms)
        stats["parse_ms"] = parse_ms
        stats["plan_ms"] = plan_ms
        stats["results_ms_sum"] = results_ms_sum

        # total_ms = parse + plan + exec (tiempo de la llamada a Executor)
        stats.setdefault("time_ms", parse_ms + plan_ms + exec_ms)

        return env