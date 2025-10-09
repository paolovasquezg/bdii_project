from backend.sql.parser import SQLParser
from backend.planner.planner import Planner
from backend.engine.executor import Executor

class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.planner = Planner()
        self.exec = Executor()

    def run(self, sql: str):
        ast = self.parser.parse(sql)      # SQL -> AST
        plans = self.planner.plan(ast)    # AST -> Planes
        return self.exec.run(plans)       # Planes -> Resultados
