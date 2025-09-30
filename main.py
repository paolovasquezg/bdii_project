from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from methods.Methods import load_tables

app = FastAPI(title="DB2 Project")

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
    return {"query": query.content}
