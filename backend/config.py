from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
TEMPLATES_DIR = FRONTEND_DIR / "templates"
STATIC_DIR = FRONTEND_DIR / "static"


DB_NAME = os.getenv("PRAMANA_DB_NAME", "pramana")
DB_USER = os.getenv("PRAMANA_DB_USER", "postgres")
DB_PASSWORD = os.getenv("PRAMANA_DB_PASSWORD", "postgres")
DB_HOST = os.getenv("PRAMANA_DB_HOST", "localhost")
DB_PORT = os.getenv("PRAMANA_DB_PORT", "5432")
