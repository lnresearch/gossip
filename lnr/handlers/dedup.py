"""Deduplication handler for gossip messages.

This handler receives messages from the raw exchange, validates them,
strips protobuf envelopes if present, checks for duplicates using SQLite,
and forwards unique messages to the uniq exchange.
"""

import logging
from typing import Optional

import aiosqlite
from faststream import context
from faststream.rabbit import RabbitMessage

from lnr.config import Config
from lnr.models import create_database
from lnr.status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


class DedupState:
    """State container for dedup handler - holds the database connection."""

    def __init__(self, config: Config):
        self.config = config
        self.db: Optional[aiosqlite.Connection] = None
        # Initialize database schema
        create_database(config.database_path)
        logger.info(f"Dedup state initialized with database at {config.database_path}")

    async def start(self) -> None:
        """Open database connection."""
        self.db = await aiosqlite.connect(self.config.database_path)

        # Enable WAL mode for better concurrency
        await self.db.execute("PRAGMA journal_mode=WAL")
        # Additional performance settings
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.execute("PRAGMA cache_size=10000")
        await self.db.execute("PRAGMA temp_store=memory")
        await self.db.commit()

        logger.info("Dedup database connection opened")

    async def stop(self) -> None:
        """Close database connection."""
        if self.db:
            await self.db.close()
            logger.info("Dedup database connection closed")

    def _is_valid_gossip_message(self, data: bytes) -> bool:
        """Check if message is valid (raw Lightning or protobuf-wrapped gossip)."""
        if len(data) < 2:
            return False

        raw_lightning_types = [b"\x01\x00", b"\x01\x01", b"\x01\x02"]

        if data[:2] in raw_lightning_types:
            return True

        if data[0:1] == b"\x0a":
            return True

        return False

    def _strip_protobuf_envelope(self, data: bytes) -> bytes:
        """Strip protobuf envelope if present, otherwise return original data."""
        if len(data) < 2:
            return data

        if data[0:1] != b"\x0a":
            return data

        try:
            offset = 1
            length = 0
            shift = 0

            while offset < len(data):
                byte = data[offset]
                length |= (byte & 0x7F) << shift
                offset += 1
                if (byte & 0x80) == 0:
                    break
                shift += 7
                if shift >= 64:
                    return data

            if offset + length <= len(data):
                inner_data = data[offset : offset + length]

                if len(inner_data) >= 2:
                    lightning_types = [b"\x01\x00", b"\x01\x01", b"\x01\x02"]
                    if inner_data[:2] in lightning_types:
                        logger.debug(
                            f"Stripped protobuf envelope: {len(data)} -> {len(inner_data)} bytes"
                        )
                        return inner_data

            return data

        except Exception as e:
            logger.debug(f"Failed to strip protobuf envelope: {e}")
            return data

    async def is_unique_message(self, data: bytes) -> bool:
        """Check if message is unique and store it if it is."""
        try:
            await self.db.execute("INSERT INTO messages (raw) VALUES (?)", (data,))
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def process_dedup_message(
    message: RabbitMessage,
    state: DedupState = context.Context(),
) -> Optional[bytes]:
    """Process a raw message and check for duplicates.

    Returns the processed message if unique, None if duplicate.
    """
    try:
        raw_data = message.body

        if not state._is_valid_gossip_message(raw_data):
            logger.debug(f"Ignoring non-gossip message of length {len(raw_data)}")
            return None

        processed_data = state._strip_protobuf_envelope(raw_data)

        if await state.is_unique_message(processed_data):
            await status_manager.increment_message_count("dedup")
            logger.debug(f"Forwarded unique message of {len(processed_data)} bytes")

            # Check if we were in error state and reset to running
            service_info = await status_manager.get_service_info("dedup")
            if service_info and service_info.status == ServiceStatus.ERROR:
                logger.info("Service recovered from error state, resetting to RUNNING")
                await status_manager.update_service_status("dedup", ServiceStatus.RUNNING)

            return processed_data
        else:
            logger.debug(f"Dropped duplicate message of {len(processed_data)} bytes")
            return None

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await status_manager.update_service_status(
            "dedup", ServiceStatus.ERROR, f"Error processing message: {e}"
        )
        raise
