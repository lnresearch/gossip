#!/usr/bin/env python3
"""CLI for managing LNR services individually."""

import asyncio
import click
import logging
from rich.console import Console
from rich.logging import RichHandler

from .config import Config
from .services.glbridge import GlbridgeService
from .services.dedup import DedupService
from .services.archiver import ArchiverService

console = Console()


def setup_logging(log_level: str = "INFO") -> None:
    """Setup rich logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )


@click.group()
@click.option('--log-level', default='INFO', help='Log level')
@click.pass_context
def cli(ctx, log_level):
    """LNR Gossip Pipeline CLI."""
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config()
    setup_logging(log_level)


@cli.command()
@click.pass_context
def glbridge(ctx):
    """Run the glbridge service."""
    config = ctx.obj['config']
    service = GlbridgeService(config)
    asyncio.run(service.run())


@cli.command()
@click.pass_context
def dedup(ctx):
    """Run the dedup service."""
    config = ctx.obj['config']
    service = DedupService(config)
    asyncio.run(service.run())


@cli.command()
@click.pass_context
def archiver(ctx):
    """Run the archiver service."""
    config = ctx.obj['config']
    service = ArchiverService(config)
    asyncio.run(service.run())


@cli.command()
@click.pass_context
def web(ctx):
    """Run the web interface with all services."""
    import uvicorn
    config = ctx.obj['config']
    uvicorn.run(
        "lnr.web:app",
        host=config.web_host,
        port=config.web_port,
        reload=False,
        log_level=config.log_level.lower()
    )


if __name__ == "__main__":
    cli()