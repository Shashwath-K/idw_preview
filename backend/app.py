from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from backend import upload
from backend.config import STATIC_DIR, TEMPLATES_DIR
from backend.routers import exposure, instructor, region, session


app = FastAPI(title="Pramana Analytics Dashboard")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.include_router(session.router)
app.include_router(exposure.router)
app.include_router(region.router)
app.include_router(instructor.router)
app.include_router(upload.router)


def render_page(request: Request, template_name: str, title: str, page_id: str):
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "page_title": title,
            "page_id": page_id,
        },
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return render_page(request, "index.html", "Dashboard", "dashboard")


@app.get("/sessions", response_class=HTMLResponse)
def sessions_page(request: Request):
    return render_page(request, "session.html", "Sessions", "sessions")


@app.get("/region-impact", response_class=HTMLResponse)
def region_page(request: Request):
    return render_page(request, "region.html", "Region Impact", "region")


@app.get("/instructor-productivity", response_class=HTMLResponse)
def instructor_page(request: Request):
    return render_page(request, "instructor.html", "Instructor Productivity", "instructor")


@app.get("/program-metrics", response_class=HTMLResponse)
def exposure_page(request: Request):
    return render_page(request, "exposure.html", "Program Metrics", "programs")
