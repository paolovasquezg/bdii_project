from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from backend.catalog.ddl import load_tables
from backend.engine.engine import Engine
from fastapi.middleware.cors import CORSMiddleware

from backend.catalog.catalog import load_tables, get_json, table_meta_path

app = FastAPI(title="DB2 Project")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)
engine = Engine()

class Query(BaseModel):
    content: str

@app.get("/")
def root():
    return "Testing: Databases 2 Project"

@app.get("/tables")
def get_tables():
    tables_names = load_tables()
    tables = {}

    for table in tables_names:

        meta = table_meta_path(table)
        relation, indexes = get_json(str(meta),2)
        tables[table] = {"relation": relation, "indexes": indexes}

    return tables

#aca debe usarse el parser
@app.post("/query")
def do_query(query: Query):
    return engine.run(query.content)