from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import psycopg
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Environment Variables
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Input schema
class QueryRequest(BaseModel):
    query: str

@app.post("/query")
async def run_query(request: QueryRequest):
    query = request.query.strip()

    # Basic protection: only allow SELECT queries
    if not query.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")

    try:
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
        return {"success": True, "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}
