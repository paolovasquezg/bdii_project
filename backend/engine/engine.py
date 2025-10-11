from time import perf_counter
from dataclasses import asdict, is_dataclass
from backend.sql.parser import SQLParser
from backend.planner.planner import Planner
from backend.engine.executor import Executor, err_result   # <-- importa err_result


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

        # 3) Ejecución: Executor ya devuelve envelope (y captura errores internos)
        return self.exec.run(plans)
