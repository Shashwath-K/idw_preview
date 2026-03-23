from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pathlib import Path

from backend import upload
from backend.routers import exposure, instructor, overview, region, session


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

TEMPLATES_DIR = PROJECT_DIR / "frontend" / "templates"
STATIC_DIR = PROJECT_DIR / "frontend" / "static"


app = FastAPI(title="Pramana Analytics Dashboard")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


app.include_router(overview.router)
app.include_router(session.router)
app.include_router(exposure.router)
app.include_router(region.router)
app.include_router(instructor.router)
app.include_router(upload.router)