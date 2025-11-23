"""FastStream broker configuration and exchange/queue definitions.

This module defines the RabbitMQ broker, exchanges, and queues used throughout
the gossip processing pipeline.
"""

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, ExchangeType
from lnr.config import Config


def create_broker(config: Config) -> RabbitBroker:
    """Create and configure the local RabbitMQ broker.

    Args:
        config: Application configuration containing RabbitMQ URL

    Returns:
        Configured RabbitBroker instance for local broker
    """
    broker = RabbitBroker(
        url=config.rabbitmq_url,
        # Enable graceful shutdown
        graceful_timeout=30.0,
        # Log level from config
        logger=None,  # Will use application logger
    )
    return broker


def create_upstream_broker(config: Config) -> RabbitBroker:
    """Create and configure the upstream RabbitMQ broker for bridge.

    Args:
        config: Application configuration containing upstream RabbitMQ URL

    Returns:
        Configured RabbitBroker instance for upstream broker
    """
    broker = RabbitBroker(
        url=config.upstream_rabbitmq_url,
        # Enable graceful shutdown
        graceful_timeout=30.0,
        # Log level from config
        logger=None,  # Will use application logger
    )
    return broker


# Exchange Definitions
# These exchanges route messages through the pipeline stages

raw_exchange = RabbitExchange(
    name="lnr.gossip.raw",
    type=ExchangeType.FANOUT,
    durable=True,
    auto_delete=False,
)
"""Raw exchange: Receives all incoming messages from bridge.

Messages from upstream RabbitMQ are published here by the glbridge handler.
The dedup service subscribes to this exchange to filter duplicates.
"""

uniq_exchange = RabbitExchange(
    name="lnr.gossip.uniq",
    type=ExchangeType.FANOUT,
    durable=True,
    auto_delete=False,
)
"""Unique exchange: Receives deduplicated messages.

Only unique messages (not seen before) are published here by the dedup handler.
The archiver service subscribes to this exchange to write messages to files.
"""


# Queue Definitions
# These queues bind to exchanges and buffer messages for handlers

dedup_queue = RabbitQueue(
    name="lnr.gossip.dedup",
    durable=True,
    auto_delete=False,
)
"""Dedup queue: Bound to raw_exchange.

This queue receives all raw messages and feeds them to the dedup handler
for duplicate filtering.
"""

archiver_queue = RabbitQueue(
    name="lnr.gossip.archiver",
    durable=True,
    auto_delete=False,
)
"""Archiver queue: Bound to uniq_exchange.

This queue receives unique messages and feeds them to the archiver handler
for writing to time-based snapshot files.
"""
