import os
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "warehouse/etl_lite.duckdb")
APP_TITLE = os.getenv("APP_TITLE", "ETL Lite – Instant Dashboards")
TZ = os.getenv("APP_TZ", "Asia/Karachi")
APP_PASSWORD = os.getenv("APP_PASSWORD", "")

# Upload/ingest controls
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "200"))  # Max file size in MB
MAX_ROWS = int(os.getenv("MAX_ROWS", "500000"))  # Max rows allowed (500K default)
MAX_COLUMNS = int(os.getenv("MAX_COLUMNS", "100"))  # Max columns allowed
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))

# CORS origins (comma separated)
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:8501,http://127.0.0.1:8501").split(",") if o.strip()]
