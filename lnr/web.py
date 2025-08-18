import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from .status import status_manager, ServiceStatus
from .config import Config
from .services.glbridge import GlbridgeService
from .services.dedup import DedupService
from .services.archiver import ArchiverService

# Global services
services: Dict[str, object] = {}
service_tasks: Dict[str, asyncio.Task] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    config = Config()
    
    logger = logging.getLogger(__name__)
    logger.info("Starting LNR Gossip Pipeline services")
    
    # Initialize services
    services["glbridge"] = GlbridgeService(config)
    services["dedup"] = DedupService(config)
    services["archiver"] = ArchiverService(config)
    
    # Start services as background tasks
    for name, service in services.items():
        logger.info(f"Starting service: {name}")
        service_tasks[name] = asyncio.create_task(
            service.run(),
            name=f"service_{name}"
        )
    
    logger.info("All services started")
    
    yield
    
    # Shutdown services
    logger.info("Shutting down services")
    
    # Cancel all service tasks
    for name, task in service_tasks.items():
        logger.info(f"Stopping service: {name}")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    
    # Stop services gracefully
    for name, service in services.items():
        if hasattr(service, 'stop'):
            await service.stop()
    
    logger.info("All services stopped")


app = FastAPI(
    title="LNR Gossip Pipeline", 
    description="Lightning Network Research Gossip Processing Pipeline",
    lifespan=lifespan
)

# Setup templates directory
templates_dir = Path(__file__).parent / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Setup static files directory
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

config = Config()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard showing service statuses."""
    services = await status_manager.get_all_services()
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "services": services,
        "title": "LNR Gossip Pipeline Dashboard"
    })


@app.get("/api/status")
async def get_status():
    """API endpoint to get all service statuses."""
    services = await status_manager.get_all_services()
    
    # Convert to dict for JSON serialization
    result = {}
    for name, info in services.items():
        result[name] = {
            "name": info.name,
            "status": info.status.value,
            "last_update": info.last_update.isoformat(),
            "message_count": info.message_count,
            "error_message": info.error_message,
            "last_message_time": info.last_message_time.isoformat() if info.last_message_time else None
        }
    
    return result


@app.get("/api/status/{service_name}")
async def get_service_status(service_name: str):
    """API endpoint to get status of a specific service."""
    info = await status_manager.get_service_info(service_name)
    
    if not info:
        return {"error": f"Service {service_name} not found"}
    
    return {
        "name": info.name,
        "status": info.status.value,
        "last_update": info.last_update.isoformat(),
        "message_count": info.message_count,
        "error_message": info.error_message,
        "last_message_time": info.last_message_time.isoformat() if info.last_message_time else None
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "services": len(await status_manager.get_all_services())}