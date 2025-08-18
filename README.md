# LNR Gossip Processing Pipeline

A Lightning Network research pipeline that ingests, deduplicates, and archives gossip messages using RabbitMQ and SQLite.

## Architecture

The pipeline consists of three main services:

1. **glbridge**: Ingests gossip messages from upstream RabbitMQ and publishes to local `lnr.gossip.raw` exchange
2. **dedup**: Deduplicates messages using SQLite and publishes unique messages to `lnr.gossip.uniq` exchange  
3. **archiver**: Archives unique messages to daily/hourly CSV snapshot files

## Installation

```bash
# Install with uv (recommended)
uv pip install -e .

# Or with pip
pip install -e .
```

## Configuration

Environment variables:
- `RABBITMQ_URL`: Local RabbitMQ URL (default: `amqp://guest:guest@localhost:5672/`)
- `UPSTREAM_RABBITMQ_URL`: Upstream RabbitMQ URL for glbridge
- `UPSTREAM_QUEUE_NAME`: Upstream queue name (default: `gossip.raw`)
- `DATABASE_PATH`: SQLite database path (default: `gossip.db`)
- `ARCHIVE_DIRECTORY`: Archive directory (default: `annex/dailies`)
- `ARCHIVE_ROTATION`: Archive rotation `hourly` or `daily` (default: `daily`)
- `WEB_HOST`: Web server host (default: `0.0.0.0`)
- `WEB_PORT`: Web server port (default: `8000`)
- `LOG_LEVEL`: Log level (default: `INFO`)

## Usage

### Web Interface (Recommended)

Run all services with web dashboard:

```bash
# Using uv
uv run python -m lnr.main

# Or using installed script
lnr-web
```

Visit http://localhost:8000 for the status dashboard.

### Individual Services

Run services individually:

```bash
# Using uv
uv run python -m lnr.cli glbridge
uv run python -m lnr.cli dedup  
uv run python -m lnr.cli archiver
uv run python -m lnr.cli web

# Or using installed script
lnr glbridge
lnr dedup
lnr archiver
lnr web
```

### Services Only (No Web Interface)

```bash
# Using uv
uv run python -m lnr.main services-only

# Or using installed script  
lnr-web services-only
```

## Message Flow

```
Upstream RabbitMQ → glbridge → lnr.gossip.raw → dedup → lnr.gossip.uniq → archiver → CSV files
```

## Development

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Format code
black .

# Run linting
ruff check .

# Type checking
mypy lnr/
```

## Lightning Network Message Types

The pipeline processes these Lightning Network gossip message types:
- `0x0100`: channel_announcement
- `0x0101`: node_announcement
- `0x0102`: channel_update