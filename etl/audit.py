import duckdb, hashlib
from datetime import datetime
from typing import Optional
from .utils import WAREHOUSE_PATH


def ensure_audit_tables():
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS etl.load_audit(
            id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            source VARCHAR,
            entity VARCHAR,
            rows_in BIGINT,
            rows_loaded BIGINT,
            error_count BIGINT,
            file_hash VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.close()


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()


def hash_filelike(f) -> str:
    pos = None
    try:
        pos = f.tell()
    except Exception:
        pos = None
    f.seek(0)
    hasher = hashlib.sha256()
    while True:
        chunk = f.read(1024 * 1024)
        if not chunk:
            break
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8", errors="ignore")
        hasher.update(chunk)
    if pos is not None:
        f.seek(pos)
    return hasher.hexdigest()


def record_audit(source: str, entity: str, rows_in: int, rows_loaded: int, error_count: int, file_hash: Optional[str]):
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("INSERT INTO etl.load_audit(source, entity, rows_in, rows_loaded, error_count, file_hash, created_at) VALUES (?,?,?,?,?,?,?)",
                [source, entity, rows_in, rows_loaded, error_count, file_hash, datetime.utcnow()])
    con.close()


