from api.sql.parser import SQLParser
from api.planner.planner import Planner
from api.engine.executor import Executor

class Engine:
    def __init__(self):
        self.parser = SQLParser()
        self.planner = Planner()
        self.exec = Executor()

    def run(self, sql: str):
        ast = self.parser.parse(sql)      # SQL -> AST
        plans = self.planner.plan(ast)    # AST -> Planes
        return self.exec.run(plans)       # Planes -> Resultados
