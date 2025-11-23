"""Main application entry point for LNR Gossip Pipeline with FastStream."""

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from faststream import context
from faststream.rabbit import RabbitBroker, RabbitExchange, ExchangeType, RabbitQueue
from rich.console import Console
from rich.logging import RichHandler

from .broker import (
    create_broker,
    create_upstream_broker,
    raw_exchange,
    uniq_exchange,
    dedup_queue,
    archiver_queue,
)
from .config import Config
from .handlers.bridge import process_bridge_message
from .handlers.dedup import DedupState, process_dedup_message
from .handlers.archiver import ArchiverState, process_archive_message
from .status import status_manager, ServiceStatus
from .web import app as web_app

console = Console()


def setup_logging(log_level: str = "INFO") -> None:
    """Setup rich logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


# Initialize config
config = Config()
setup_logging(config.log_level)
logger = logging.getLogger(__name__)

# Create brokers
broker = create_broker(config)
upstream_broker = create_upstream_broker(config)

# Create state objects
dedup_state = DedupState(config)
archiver_state = ArchiverState(config)

# Define upstream exchange for bridge
upstream_exchange = RabbitExchange(
    name=config.upstream_queue_name,  # e.g., "router.gossip"
    type=ExchangeType.FANOUT,
    passive=True,  # Don't create, just connect to existing
)

# Create upstream queue for bridge (temporary, exclusive)
upstream_queue = RabbitQueue(
    name="lnr.bridge",
    exclusive=True,
    auto_delete=True,
)


# Wire up handlers with decorators
@upstream_broker.subscriber(queue=upstream_queue, exchange=upstream_exchange)
@broker.publisher(exchange=raw_exchange)
async def bridge_handler(msg: bytes = context.Context("message.body")):
    """Bridge handler: upstream -> local raw exchange."""
    from faststream.rabbit import RabbitMessage

    # Create a RabbitMessage wrapper for the handler
    message = RabbitMessage(body=msg)
    result = await process_bridge_message(message)
    return result  # FastStream will publish if not None


@broker.subscriber(queue=dedup_queue, exchange=raw_exchange)
@broker.publisher(exchange=uniq_exchange)
async def dedup_handler(msg: bytes = context.Context("message.body")):
    """Dedup handler: raw exchange -> uniq exchange."""
    from faststream.rabbit import RabbitMessage

    message = RabbitMessage(body=msg)
    result = await process_dedup_message(message, dedup_state)
    return result  # FastStream will publish if not None


@broker.subscriber(queue=archiver_queue, exchange=uniq_exchange)
async def archiver_handler(msg: bytes = context.Context("message.body")):
    """Archiver handler: uniq exchange -> file."""
    from faststream.rabbit import RabbitMessage

    message = RabbitMessage(body=msg)
    await process_archive_message(message, archiver_state)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting LNR Gossip Pipeline with FastStream")

    # Update service status
    await status_manager.update_service_status("glbridge", ServiceStatus.STARTING)
    await status_manager.update_service_status("dedup", ServiceStatus.STARTING)
    await status_manager.update_service_status("archiver", ServiceStatus.STARTING)

    # Start state objects
    await dedup_state.start()
    await archiver_state.start()

    # Start brokers
    await broker.start()
    await upstream_broker.start()

    # Update status to running
    await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)
    await status_manager.update_service_status("dedup", ServiceStatus.RUNNING)
    await status_manager.update_service_status("archiver", ServiceStatus.RUNNING)

    logger.info("All services started successfully")

    yield

    # Shutdown
    logger.info("Shutting down services")

    # Stop brokers
    await upstream_broker.close()
    await broker.close()

    # Stop state objects
    await dedup_state.stop()
    await archiver_state.stop()

    # Update status
    await status_manager.update_service_status("glbridge", ServiceStatus.STOPPED)
    await status_manager.update_service_status("dedup", ServiceStatus.STOPPED)
    await status_manager.update_service_status("archiver", ServiceStatus.STOPPED)

    logger.info("All services stopped")


# Update the web app with lifespan
web_app.router.lifespan_context = lifespan


async def run_services_only():
    """Run only the services without the web interface."""
    logger.info("Starting services-only mode with FastStream")

    # Update service status
    await status_manager.update_service_status("glbridge", ServiceStatus.STARTING)
    await status_manager.update_service_status("dedup", ServiceStatus.STARTING)
    await status_manager.update_service_status("archiver", ServiceStatus.STARTING)

    # Start state objects
    await dedup_state.start()
    await archiver_state.start()

    # Start brokers
    await broker.start()
    await upstream_broker.start()

    # Update status to running
    await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)
    await status_manager.update_service_status("dedup", ServiceStatus.RUNNING)
    await status_manager.update_service_status("archiver", ServiceStatus.RUNNING)

    logger.info("All services started successfully")

    try:
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        # Shutdown
        logger.info("Shutting down services")

        # Stop brokers
        await upstream_broker.close()
        await broker.close()

        # Stop state objects
        await dedup_state.stop()
        await archiver_state.stop()

        # Update status
        await status_manager.update_service_status("glbridge", ServiceStatus.STOPPED)
        await status_manager.update_service_status("dedup", ServiceStatus.STOPPED)
        await status_manager.update_service_status("archiver", ServiceStatus.STOPPED)

        logger.info("All services stopped")


def main():
    """Main entry point for the application."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "services-only":
        # Run only services without web interface
        asyncio.run(run_services_only())
    else:
        # Run with web interface
        uvicorn.run(
            "lnr.web:app",
            host=config.web_host,
            port=config.web_port,
            reload=False,
            log_level=config.log_level.lower(),
        )


if __name__ == "__main__":
    main()
