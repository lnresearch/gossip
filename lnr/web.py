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
from .services.syncer import SyncerService
from .stats import stats_counter

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
    services["syncer"] = SyncerService(config)
    
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
    global services
    service_infos = await status_manager.get_all_services()
    
    # Convert to dict for JSON serialization
    result = {}
    for name, info in service_infos.items():
        service_data = {
            "name": info.name,
            "status": info.status.value,
            "last_update": info.last_update.isoformat(),
            "message_count": info.message_count,
            "error_message": info.error_message,
            "last_message_time": info.last_message_time.isoformat() if info.last_message_time else None
        }
        
        # Add archiver-specific fields
        if name == "archiver":
            service_data.update({
                "current_archive_messages": info.current_archive_messages,
                "current_archive_bytes": info.current_archive_bytes,
                "current_archive_bytes_human": _format_bytes(info.current_archive_bytes),
            })

            # Calculate time to next flush using global services instance
            archiver_service = services.get("archiver")
            if archiver_service and hasattr(archiver_service, 'calculate_next_flush_time'):
                next_flush = archiver_service.calculate_next_flush_time()
                if next_flush:
                    service_data["next_flush_time"] = next_flush.isoformat()
                    from datetime import datetime, timezone
                    now = datetime.now(timezone.utc)
                    time_to_flush = next_flush - now
                    service_data["time_to_next_flush_seconds"] = int(time_to_flush.total_seconds())
                    service_data["time_to_next_flush_human"] = _format_time_duration(time_to_flush.total_seconds())

        # Add syncer-specific fields
        elif name == "syncer":
            service_data.update({
                "annex_total_files": info.annex_total_files,
                "annex_local_files": info.annex_local_files,
                "annex_remote_files": info.annex_remote_files,
                "annex_total_size": info.annex_total_size,
                "annex_total_size_human": _format_bytes(info.annex_total_size),
                "last_sync_time": info.last_sync_time.isoformat() if info.last_sync_time else None,
            })
        
        result[name] = service_data
    
    return result


def _format_bytes(bytes_count: int) -> str:
    """Format bytes into human readable form."""
    if bytes_count < 1024:
        return f"{bytes_count} B"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f} KB"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.1f} GB"


def _format_time_duration(seconds: float) -> str:
    """Format duration in seconds to human readable form."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


@app.get("/api/status/{service_name}")
async def get_service_status(service_name: str):
    """API endpoint to get status of a specific service."""
    info = await status_manager.get_service_info(service_name)
    
    if not info:
        return {"error": f"Service {service_name} not found"}
    
    service_data = {
        "name": info.name,
        "status": info.status.value,
        "last_update": info.last_update.isoformat(),
        "message_count": info.message_count,
        "error_message": info.error_message,
        "last_message_time": info.last_message_time.isoformat() if info.last_message_time else None
    }
    
    # Add archiver-specific fields
    if service_name == "archiver":
        service_data.update({
            "current_archive_messages": info.current_archive_messages,
            "current_archive_bytes": info.current_archive_bytes,
            "current_archive_bytes_human": _format_bytes(info.current_archive_bytes),
        })

        # Calculate time to next flush
        global services
        archiver_service = services.get("archiver")
        if archiver_service and hasattr(archiver_service, 'calculate_next_flush_time'):
            next_flush = archiver_service.calculate_next_flush_time()
            if next_flush:
                service_data["next_flush_time"] = next_flush.isoformat()
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                time_to_flush = next_flush - now
                service_data["time_to_next_flush_seconds"] = int(time_to_flush.total_seconds())
                service_data["time_to_next_flush_human"] = _format_time_duration(time_to_flush.total_seconds())

    # Add syncer-specific fields
    elif service_name == "syncer":
        service_data.update({
            "annex_total_files": info.annex_total_files,
            "annex_local_files": info.annex_local_files,
            "annex_remote_files": info.annex_remote_files,
            "annex_total_size": info.annex_total_size,
            "annex_total_size_human": _format_bytes(info.annex_total_size),
            "last_sync_time": info.last_sync_time.isoformat() if info.last_sync_time else None,
        })

    return service_data


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "services": len(await status_manager.get_all_services())}


@app.get("/api/stats")
async def get_all_stats():
    """Get all statistics from all handlers.

    Returns:
        Dictionary of all statistics tracked by StatsCounter
    """
    return stats_counter.get_all()


@app.get("/api/stats/formatted")
async def get_formatted_stats():
    """Get formatted statistics for display in the UI.

    Returns:
        Dictionary with formatted stats for each handler
    """
    from datetime import datetime, timezone

    all_stats = stats_counter.get_all()

    def format_last_processed(timestamp_str):
        if not timestamp_str:
            return "Never"
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            delta = now - timestamp

            if delta.total_seconds() < 60:
                return f"{int(delta.total_seconds())}s ago"
            elif delta.total_seconds() < 3600:
                return f"{int(delta.total_seconds() / 60)}m ago"
            elif delta.total_seconds() < 86400:
                return f"{int(delta.total_seconds() / 3600)}h ago"
            else:
                return f"{int(delta.total_seconds() / 86400)}d ago"
        except:
            return "Unknown"

    def format_duration(seconds):
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}m {secs}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"

    # Bridge stats
    bridge_stats = {
        "incoming": all_stats.get("bridge.incoming", 0),
        "published": all_stats.get("bridge.published", 0),
        "invalid": all_stats.get("bridge.invalid", 0),
        "errors": all_stats.get("bridge.errors", 0),
        "publish_errors": all_stats.get("bridge.publish_errors", 0),
        "last_processed": format_last_processed(all_stats.get("bridge.last_processed_time")),
        "last_processed_raw": all_stats.get("bridge.last_processed_time"),
    }

    # Dedup stats
    dedup_stats = {
        "incoming": all_stats.get("dedup.incoming", 0),
        "outgoing": all_stats.get("dedup.outgoing", 0),
        "db_size": all_stats.get("dedup.db_size", 0),
        "duplicates": all_stats.get("dedup.duplicates", 0),
        "errors": all_stats.get("dedup.errors", 0),
        "last_processed": format_last_processed(all_stats.get("dedup.last_processed_time")),
        "last_processed_raw": all_stats.get("dedup.last_processed_time"),
    }

    # Archiver stats
    seconds_since_rotation = all_stats.get("archiver.seconds_since_rotation", 0)
    archiver_stats = {
        "incoming": all_stats.get("archiver.incoming", 0),
        "current_file_messages": all_stats.get("archiver.current_file_messages", 0),
        "current_file_bytes": all_stats.get("archiver.current_file_bytes", 0),
        "current_file_bytes_human": _format_bytes(all_stats.get("archiver.current_file_bytes", 0)),
        "current_file": all_stats.get("archiver.current_file", "N/A"),
        "seconds_since_rotation": seconds_since_rotation,
        "time_since_rotation": format_duration(seconds_since_rotation),
        "last_rotation": format_last_processed(all_stats.get("archiver.last_rotation_time")),
        "last_processed": format_last_processed(all_stats.get("archiver.last_processed_time")),
        "last_processed_raw": all_stats.get("archiver.last_processed_time"),
        "errors": all_stats.get("archiver.errors", 0),
    }

    return {
        "bridge": bridge_stats,
        "dedup": dedup_stats,
        "archiver": archiver_stats,
    }


@app.get("/api/stats/{handler}")
async def get_handler_stats(handler: str):
    """Get statistics for a specific handler.

    Args:
        handler: Handler name (e.g., "bridge", "dedup", "archiver")

    Returns:
        Dictionary of statistics for the specified handler
    """
    return stats_counter.get_filtered(f"{handler}.")