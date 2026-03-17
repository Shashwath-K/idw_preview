from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
TEMPLATES_DIR = FRONTEND_DIR / "templates"
STATIC_DIR = FRONTEND_DIR / "static"
SQL_DIR = BASE_DIR / "sql"


DB_USER = os.getenv("PRAMANA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("PRAMANA_DB_PASSWORD", "postgres")
DB_HOST = os.getenv("PRAMANA_DB_HOST", "localhost")
DB_PORT = os.getenv("PRAMANA_DB_PORT", "5432")

ADMIN_DB_NAME = os.getenv("PRAMANA_ADMIN_DB_NAME", "postgres")
SOURCE_DB_NAME = os.getenv("PRAMANA_SOURCE_DB_NAME", "pramana_source")
DATAMART_DB_NAME = os.getenv("PRAMANA_DATAMART_DB_NAME", "pramana_idw")
FDW_SOURCE_SCHEMA = os.getenv("PRAMANA_FDW_SOURCE_SCHEMA", "source_fdw")

MANAGED_SOURCE_TABLES = (
    "mst_donor",
    "mst_program",
    "mst_school",
    "mst_instructor",
    "mst_activity_type",
    "mst_shift",
    "conf_program_school_mapping",
    "txn_session",
    "txn_feedback_answer",
    "txn_feedback_exposure",
    "mst_adhoc_session_feedback_answers",
)
