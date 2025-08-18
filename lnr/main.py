import asyncio
import logging
import signal
from contextlib import asynccontextmanager
from typing import Dict, Optional
import uvicorn
from fastapi import FastAPI
from rich.console import Console
from rich.logging import RichHandler

from .config import Config
from .services.glbridge import GlbridgeService
from .services.dedup import DedupService
from .services.archiver import ArchiverService
from .web import app as web_app
from .status import status_manager

# Global services dictionary
services: Dict[str, object] = {}
service_tasks: Dict[str, asyncio.Task] = {}

console = Console()


def setup_logging(log_level: str = "INFO") -> None:
    """Setup rich logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    config = Config()
    setup_logging(config.log_level)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting LNR Gossip Pipeline")
    
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


# Update the web app with lifespan
web_app.router.lifespan_context = lifespan


async def run_services_only():
    """Run only the services without the web interface."""
    config = Config()
    setup_logging(config.log_level)
    
    logger = logging.getLogger(__name__)
    
    # Initialize services
    glbridge = GlbridgeService(config)
    dedup = DedupService(config)
    archiver = ArchiverService(config)
    
    services_list = [
        ("glbridge", glbridge),
        ("dedup", dedup),
        ("archiver", archiver)
    ]
    
    # Setup signal handlers
    def signal_handler():
        logger.info("Received shutdown signal")
        for name, service in services_list:
            if hasattr(service, 'running'):
                service.running = False
    
    # Register signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: signal_handler())
    
    # Start services
    tasks = []
    for name, service in services_list:
        logger.info(f"Starting service: {name}")
        task = asyncio.create_task(service.run(), name=f"service_{name}")
        tasks.append(task)
    
    try:
        # Wait for all services
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        # Cleanup
        for task in tasks:
            task.cancel()
        
        for name, service in services_list:
            if hasattr(service, 'stop'):
                await service.stop()


def main():
    """Main entry point for the application."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "services-only":
        # Run only services without web interface
        asyncio.run(run_services_only())
    else:
        # Run with web interface
        config = Config()
        uvicorn.run(
            "lnr.web:app",
            host=config.web_host,
            port=config.web_port,
            reload=False,
            log_level=config.log_level.lower()
        )


if __name__ == "__main__":
    main()