# LNR Gossip Processing Pipeline

A Lightning Network research pipeline that ingests, deduplicates, and archives gossip messages using RabbitMQ and SQLite.

## Architecture

The pipeline consists of three main services:

1. **glbridge**: Ingests gossip messages from upstream RabbitMQ and publishes to local `lnr.gossip.raw` exchange
2. **dedup**: Deduplicates messages using SQLite and publishes unique messages to `lnr.gossip.uniq` exchange  
3. **archiver**: Archives unique messages to daily/hourly GSP\x01 format snapshot files

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
Upstream RabbitMQ → glbridge → lnr.gossip.raw → dedup → lnr.gossip.uniq → archiver → GSP files
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

## Archive File Format (GSP\x01)

Archive files use the GSP\x01 binary format for efficient storage of Lightning Network gossip messages.

### Format Structure

```
GSP File := Header + Messages*
Header   := "GSP\x01" (4 bytes, version 1)
Messages := CompactSize(length) + message_data
```

### CompactSize Encoding

CompactSize is a variable-length integer encoding format from the Bitcoin protocol:

| Value Range | First Byte | Additional Bytes | Byte Order |
|-------------|------------|------------------|------------|  
| 0-252       | value      | none             | -          |
| 253-65535   | 253 (0xFD) | 2 bytes          | big-endian |
| 65536-4294967295 | 254 (0xFE) | 4 bytes     | big-endian |
| 4294967296+ | 255 (0xFF) | 8 bytes          | big-endian |

### Example

```
File Header: GSP\x01 (0x47 0x53 0x50 0x01)
Message 1:   CompactSize(138) + 138 bytes of Lightning gossip data
Message 2:   CompactSize(150) + 150 bytes of Lightning gossip data
...
```

### Lightning Network Message Types

The pipeline processes these Lightning Network gossip message types:
- `0x0100`: channel_announcement  
- `0x0101`: node_announcement
- `0x0102`: channel_update