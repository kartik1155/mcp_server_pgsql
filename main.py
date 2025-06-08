# from fastapi import FastAPI, HTTPException, Request
# from pydantic import BaseModel
# import psycopg
# import os
# from dotenv import load_dotenv
#
# load_dotenv()
#
# app = FastAPI()
#
# # Environment Variables
# DB_PARAMS = {
#     "host": os.getenv("DB_HOST"),
#     "port": int(os.getenv("DB_PORT")),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD")
# }
#
# # Input schema
# class QueryRequest(BaseModel):
#     query: str
#
# @app.post("/query")
# async def run_query(request: QueryRequest):
#     query = request.query.strip()
#
#     # Basic protection: only allow SELECT queries
#     if not query.lower().startswith("select"):
#         raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
#
#     try:
#         with psycopg.connect(**DB_PARAMS) as conn:
#             with conn.cursor() as cur:
#                 cur.execute(query)
#                 columns = [desc[0] for desc in cur.description]
#                 rows = cur.fetchall()
#                 results = [dict(zip(columns, row)) for row in rows]
#         return {"success": True, "data": results}
#     except Exception as e:
#         return {"success": False, "error": str(e)}
#--------------------------------------------------------------------------------------------------
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

app = FastAPI()

# Database parameters
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Connect to DB
def get_db_connection():
    return psycopg.connect(**DB_PARAMS)

# Request schema for /query
class QueryRequest(BaseModel):
    query: str

@app.post("/query")
async def run_query(request: QueryRequest):
    query = request.query.strip()
    if not query.lower().startswith("select"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
        return {"success": True, "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}

# Generic fetch-all and fetch-by-ID
def fetch_all_from(table):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table}")
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def fetch_by_id(table, id_column, id_value):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table} WHERE {id_column} = %s", (id_value,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"{table} not found")
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== Table Endpoints ==========

@app.get("/users")
def get_users():
    return fetch_all_from("users")

@app.get("/users/{user_id}")
def get_user(user_id: str):
    return fetch_by_id("users", "user_id", user_id)

@app.get("/providers")
def get_providers():
    return fetch_all_from("providers")

@app.get("/providers/{provider_id}")
def get_provider(provider_id: str):
    return fetch_by_id("providers", "provider_id", provider_id)

@app.get("/offerings")
def get_offerings():
    return fetch_all_from("offerings")

@app.get("/offerings/{offering_id}")
def get_offering(offering_id: str):
    return fetch_by_id("offerings", "offering_id", offering_id)

@app.get("/service_offerings")
def get_service_offerings():
    return fetch_all_from("service_offerings")

@app.get("/service_offerings/{service_offering_id}")
def get_service_offering(service_offering_id: str):
    return fetch_by_id("service_offerings", "service_offering_id", service_offering_id)

@app.get("/bookings")
def get_bookings():
    return fetch_all_from("bookings")

@app.get("/bookings/{booking_id}")
def get_booking(booking_id: str):
    return fetch_by_id("bookings", "booking_id", booking_id)

@app.get("/service_types")
def get_service_types():
    return fetch_all_from("service_types")

@app.get("/service_types/{type_id}")
def get_service_type(type_id: str):
    return fetch_by_id("service_types", "type_id", type_id)

@app.get("/service_categories")
def get_service_categories():
    return fetch_all_from("service_categories")

@app.get("/service_categories/{category_id}")
def get_service_category(category_id: str):
    return fetch_by_id("service_categories", "category_id", category_id)

@app.get("/conversations")
def get_conversations():
    return fetch_all_from("conversations")

@app.get("/conversations/{message_id}")
def get_conversation(message_id: str):
    return fetch_by_id("conversations", "message_id", message_id)
