from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="DB2 Project")

app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def read_root():
    return "Testing: Databases 2 Project"
