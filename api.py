import os, time, duckdb
from datetime import datetime, date
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from etl.utils import WAREHOUSE_PATH
from etl.utils import CORS_ORIGINS
from etl.load import init_warehouse

API_TOKEN = os.getenv("API_TOKEN", "changeme")
app = FastAPI(title="ETL-Lite API", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"]
)

def auth(token: str):
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

def run_with_retry(fn, retries: int = 3, delay: float = 0.2):
    for i in range(retries):
        try:
            return fn()
        except duckdb.Error:
            if i == retries - 1:
                raise
            time.sleep(delay * (i + 1))

class Sale(BaseModel):
    date: datetime
    product_name: Optional[str] = None
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    amount: Optional[float] = None

class Expense(BaseModel):
    date: datetime
    expense_category: str
    amount: float

@app.on_event("startup")
def startup():
    init_warehouse()

@app.exception_handler(Exception)
async def all_errors(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest/sales")
def ingest_sales(items: List[Sale], token: str):
    auth(token)
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("CREATE TEMP TABLE t(date TIMESTAMP, product_name VARCHAR, quantity DOUBLE, unit_price DOUBLE, amount DOUBLE)")
    for it in items:
        amt = it.amount if it.amount is not None else ((it.quantity or 0) * (it.unit_price or 0))
        con.execute("INSERT INTO t VALUES (?,?,?,?,?)", [it.date, it.product_name, it.quantity, it.unit_price, amt])
    run_with_retry(lambda: con.execute("CREATE TABLE IF NOT EXISTS etl.sales AS SELECT * FROM t LIMIT 0"))
    run_with_retry(lambda: con.execute("INSERT INTO etl.sales SELECT * FROM t"))
    con.close()
    return {"inserted": len(items)}

@app.post("/ingest/expenses")
def ingest_expenses(items: List[Expense], token: str):
    auth(token)
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("CREATE TEMP TABLE t(date TIMESTAMP, expense_category VARCHAR, amount DOUBLE)")
    for it in items:
        con.execute("INSERT INTO t VALUES (?,?,?)", [it.date, it.expense_category, it.amount])
    run_with_retry(lambda: con.execute("CREATE TABLE IF NOT EXISTS etl.expenses AS SELECT * FROM t LIMIT 0"))
    run_with_retry(lambda: con.execute("INSERT INTO etl.expenses SELECT * FROM t"))
    con.close()
    return {"inserted": len(items)}

@app.get("/metrics/summary")
def metrics_summary(start: Optional[date] = None, end: Optional[date] = None, token: str = ""):
    auth(token)
    con = duckdb.connect(WAREHOUSE_PATH)
    where, params = [], []
    if start:
        where.append("date::date >= ?"); params.append(start)
    if end:
        where.append("date::date <= ?"); params.append(end)
    ws = " AND ".join(where)
    sales_q = f"SELECT COALESCE(SUM(amount),0) FROM etl.sales { 'WHERE ' + ws if ws else '' }"
    exp_q = f"SELECT COALESCE(SUM(amount),0) FROM etl.expenses { 'WHERE ' + ws if ws else '' }"
    sales = con.execute(sales_q, params).fetchone()[0]
    expenses = con.execute(exp_q, params).fetchone()[0]
    con.close()
    return {"sales": sales, "expenses": expenses, "profit": sales - expenses}
