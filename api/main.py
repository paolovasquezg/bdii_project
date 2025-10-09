from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from catalog.ddl import load_tables
from engine.engine import Engine

app = FastAPI(title="DB2 Project")
engine = Engine()

app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class Query(BaseModel):
    content: str

@app.get("/")
def root():
    return "Testing: Databases 2 Project"

@app.get("/tables")
def get_tables():
    tables_names = load_tables()
    tables = []

    for table in tables_names:
        tables.append(table)

    return tables

#aca debe usarse el parser
@app.post("/query")
def do_query(query: Query):
    try:
        return {"ok": True, "result": engine.run(query.content)}
    except Exception as e:
        raise HTTPException(400, str(e))
