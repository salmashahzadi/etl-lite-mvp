from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from etl.utils import WAREHOUSE_PATH
from etl.utils import CORS_ORIGINS, MAX_UPLOAD_MB, CHUNK_SIZE
from etl.ingest import read_file_to_df, suggest_column_mapping
from etl.transform import transform_with_mapping
from etl.validate import validate_dataframe, summarize_errors
from etl.audit import ensure_audit_tables, record_audit, hash_filelike
from etl.load import safe_load_to_duckdb

app = FastAPI(title="ETL-Lite API EXT", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"]
)

@app.post("/ingest/csv")
def ingest_csv(entity: str = Form(...), mode: str = Form("append"), file: UploadFile = File(...)):
    if entity not in ("sales", "expenses", "appointments"):
        raise HTTPException(status_code=400, detail="entity must be 'sales' or 'expenses' or 'appointments'")
    if file.size and file.size > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"File too large. Max {MAX_UPLOAD_MB} MB")
    # Idempotency: compute hash of uploaded file content
    file_hash = hash_filelike(file.file)
    df = read_file_to_df(file.file)
    from etl.schema import ENTITY_SPECS
    if entity not in ENTITY_SPECS:
        raise HTTPException(status_code=400, detail="Unknown entity")
    mapping = suggest_column_mapping(df, entity)
    tdf = transform_with_mapping(df, entity, mapping)
    tdf, errs = validate_dataframe(entity, tdf)

    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.register("df", tdf)
    if mode == "replace":
        con.execute(f"DROP TABLE IF EXISTS etl.{entity}")
    con.execute(f"CREATE TABLE IF NOT EXISTS etl.{entity} AS SELECT * FROM df LIMIT 0")
    # Idempotent insert: prevent reloading same file
    ensure_audit_tables()
    con.execute("CREATE TABLE IF NOT EXISTS etl._ingest_hashes(hash VARCHAR PRIMARY KEY)")
    already = con.execute("SELECT 1 FROM etl._ingest_hashes WHERE hash = ?", [file_hash]).fetchone()
    inserted = 0
    if not already:
        # Use safe loader to evolve schema if needed
        con.close()
        safe_load_to_duckdb(tdf, f"etl.{entity}")
        con = duckdb.connect(WAREHOUSE_PATH)
        con.execute("INSERT INTO etl._ingest_hashes VALUES (?)", [file_hash])
        inserted = len(tdf)
    con.close()

    # Audit record
    record_audit(source="api_ext.csv", entity=entity, rows_in=len(df), rows_loaded=inserted, error_count=len(errs), file_hash=file_hash)

    return {"rows_in": len(df), "inserted": inserted, "entity": entity, "auto_mapping": mapping, "validation_errors": summarize_errors(errs)}
