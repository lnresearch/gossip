"""Bridge handler for upstream gossip messages.

This handler bridges messages from an upstream RabbitMQ broker to the local broker.
It subscribes to the upstream exchange and publishes to the local raw exchange.
"""

import logging
from typing import Optional

from faststream import context
from faststream.rabbit import RabbitMessage

from lnr.config import Config
from lnr.status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


def _is_valid_gossip_message(data: bytes) -> bool:
    """Check if message is valid (raw Lightning or protobuf-wrapped gossip)."""
    if len(data) < 2:
        return False

    # Raw Lightning Network gossip message types
    raw_lightning_types = [b"\x01\x00", b"\x01\x01", b"\x01\x02"]

    if data[:2] in raw_lightning_types:
        return True

    # Check for protobuf-wrapped messages
    if data[0:1] == b"\x0a":
        return True

    # Accept all messages and let downstream dedup handle validation
    return True


async def process_bridge_message(message: RabbitMessage) -> Optional[bytes]:
    """Process a message from upstream and forward to local exchange.

    Returns the message body if valid, None if should be filtered.
    """
    try:
        if not _is_valid_gossip_message(message.body):
            logger.debug(f"Ignoring non-gossip message of length {len(message.body)}")
            return None

        # Check if we were in error state and reset to running
        service_info = await status_manager.get_service_info("glbridge")
        if service_info and service_info.status == ServiceStatus.ERROR:
            logger.info("Service recovered from error state, resetting to RUNNING")
            await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)

        await status_manager.increment_message_count("glbridge")
        logger.debug(f"Forwarded gossip message of {len(message.body)} bytes")

        return message.body

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await status_manager.update_service_status(
            "glbridge", ServiceStatus.ERROR, f"Error processing message: {e}"
        )
        raise
