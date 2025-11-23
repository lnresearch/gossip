"""Archiver handler for gossip messages.

This handler receives unique messages from the uniq exchange and writes them
to time-based GSP snapshot files with compression and GCS upload.
"""

import asyncio
import bz2
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, BinaryIO

from faststream import context, Depends
from faststream.rabbit import RabbitMessage

from lnr.config import Config
from lnr.gsp import GSPWriter, GSP_HEADER, encode_compact_size
from lnr.status import status_manager, ServiceStatus
from lnr.stats import StatsCounter, stats_counter

logger = logging.getLogger(__name__)


class ArchiverState:
    """State container for archiver handler - holds file handles and rotation state."""

    def __init__(self, config: Config):
        self.config = config
        self.current_file: Optional[BinaryIO] = None
        self.gsp_writer: Optional[GSPWriter] = None
        self.current_file_path: Optional[Path] = None
        self.last_rotation_time: Optional[datetime] = None

        # Track current archive statistics
        self.current_archive_message_count = 0
        self.current_archive_byte_count = 0

        # Mutex to protect file access and rotation
        self.file_lock = asyncio.Lock()

        # Ensure directories exist
        Path(config.temp_directory).mkdir(parents=True, exist_ok=True)
        Path(config.annex_daily_directory).mkdir(parents=True, exist_ok=True)
        Path(config.database_path).parent.mkdir(parents=True, exist_ok=True)

        logger.info("Archiver state initialized")

    async def start(self) -> None:
        """Initialize first archive file."""
        await self._rotate_file_if_needed()
        logger.info("Archiver started")

    async def stop(self) -> None:
        """Close current file."""
        if self.current_file:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
            self.gsp_writer = None
            logger.info(f"Closed archive file: {self.current_file_path}")

    async def _rotate_file_if_needed(self) -> None:
        """Rotate archive file if needed based on time."""
        now = datetime.now(timezone.utc)

        should_rotate = False

        if self.last_rotation_time is None:
            should_rotate = True
        elif self.config.archive_rotation == "hourly":
            should_rotate = (
                now.hour != self.last_rotation_time.hour
                or now.date() != self.last_rotation_time.date()
            )
        else:  # daily
            should_rotate = now.date() != self.last_rotation_time.date()

        if should_rotate:
            await self._rotate_file(now)

    async def _rotate_file(self, timestamp: datetime) -> None:
        """Rotate to a new archive file."""
        old_file_path = None

        if self.current_file:
            old_file_path = self.current_file_path
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
            self.gsp_writer = None
            logger.info(f"Closed archive file: {old_file_path}")

            if old_file_path and old_file_path.exists():
                await self._move_to_annex(old_file_path)

        # Generate new filename
        if self.config.archive_rotation == "hourly":
            filename = f"gossip-{timestamp.strftime('%Y%m%d%H%M')}.gsp"
        else:  # daily
            filename = f"gossip-{timestamp.strftime('%Y%m%d')}0000.gsp"

        self.current_file_path = Path(self.config.temp_directory) / filename

        loop = asyncio.get_event_loop()
        self.current_file = await loop.run_in_executor(
            None, lambda: open(self.current_file_path, "ab")
        )

        self.gsp_writer = GSPWriter(self.current_file)
        self.last_rotation_time = timestamp
        logger.info(f"Opened new archive file: {self.current_file_path}")

        self.current_archive_message_count = 0
        self.current_archive_byte_count = 0

        if self.current_file_path.stat().st_size == 0:
            self.current_archive_byte_count = len(GSP_HEADER)

        await status_manager.update_archiver_stats(
            self.current_archive_message_count, self.current_archive_byte_count
        )

    def _write_gsp_message(self, message_data: bytes) -> None:
        """Write GSP message to current file (blocking operation)."""
        if self.gsp_writer:
            self.gsp_writer.write_message(message_data)
            self.gsp_writer.flush()

    async def write_message(self, message_data: bytes) -> None:
        """Write a message to the archive file."""
        async with self.file_lock:
            await self._rotate_file_if_needed()

            if self.gsp_writer:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._write_gsp_message, message_data)

                message_size = len(message_data)
                compact_size_bytes = len(encode_compact_size(message_size))
                total_bytes = compact_size_bytes + message_size

                self.current_archive_message_count += 1
                self.current_archive_byte_count += total_bytes

                await status_manager.increment_message_count("archiver")
                await status_manager.update_archiver_stats(
                    self.current_archive_message_count, self.current_archive_byte_count
                )

                logger.debug(f"Archived message of {len(message_data)} bytes")

    async def _move_to_annex(self, file_path: Path) -> None:
        """Move rotated file to annex directory for independent upload processing."""
        try:
            loop = asyncio.get_event_loop()
            annex_path = Path(self.config.annex_daily_directory) / file_path.name

            await loop.run_in_executor(None, shutil.move, str(file_path), str(annex_path))
            logger.info(f"Moved archive file to annex: {annex_path}")

        except Exception as e:
            logger.error(f"Failed to move file to annex {file_path}: {e}")
            raise

    def _compress_file(self, file_path: Path) -> Path:
        """Compress file with bzip2 (blocking operation)."""
        compressed_path = file_path.with_suffix(file_path.suffix + ".bz2")

        logger.info(f"Compressing {file_path} to {compressed_path}")

        with open(file_path, "rb") as f_in:
            with bz2.BZ2File(compressed_path, "wb", compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out)

        original_size = file_path.stat().st_size
        compressed_size = compressed_path.stat().st_size
        compression_ratio = compressed_size / original_size if original_size > 0 else 1.0

        logger.info(
            f"Compression completed: {original_size} -> {compressed_size} bytes "
            f"(ratio: {compression_ratio:.2f})"
        )

        return compressed_path

    def _upload_to_gcs(self, file_path: str, filename: str) -> None:
        """Upload file to GCS using Python client (blocking operation)."""
        from google.cloud import storage

        bucket_name = "lnresearch"
        blob_name = f"daily/{filename}"

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        with open(file_path, "rb") as f:
            blob.upload_from_file(f)

        logger.info(f"Successfully uploaded {file_path} to gs://{bucket_name}/{blob_name}")

    def _add_to_git_annex(self, filename: str, gcs_url: str) -> None:
        """Add file URL to git-annex (blocking operation)."""
        import os

        annex_dir = Path(self.config.annex_directory)
        original_cwd = Path.cwd()
        os.chdir(annex_dir)

        try:
            annex_file_path = f"daily/{filename}"
            subprocess.run(
                ["git", "annex", "addurl", gcs_url, "--file", annex_file_path],
                capture_output=True,
                text=True,
                check=True,
            )
            logger.info(f"Added {annex_file_path} to git-annex with URL {gcs_url}")

            subprocess.run(
                ["git", "add", annex_file_path], capture_output=True, text=True, check=True
            )

            commit_msg = (
                f"Add gossip archive {filename}\n\n"
                "Automatic archive upload from lnr archiver service"
            )
            subprocess.run(
                ["git", "commit", "-m", commit_msg], capture_output=True, text=True, check=True
            )

            subprocess.run(
                ["git", "push", self.config.github_remote],
                capture_output=True,
                text=True,
                check=True,
            )
            logger.info(f"Committed and pushed {filename} to git-annex")

        finally:
            os.chdir(original_cwd)


async def process_archive_message(
    message: bytes,
    state: ArchiverState = context.Context(),
) -> None:
    """Process a unique message and write to archive file.

    Args:
        message: Incoming message from uniq exchange
        state: Archiver state (injected via Context)
    """
    try:
        # Track incoming message
        stats_counter.increment("archiver.incoming")
        stats_counter.increment("archiver.bytes_written", len(message))

        await state.write_message(message)

        # Track current file stats
        stats_counter.set("archiver.current_file_messages", state.current_archive_message_count)
        stats_counter.set("archiver.current_file_bytes", state.current_archive_byte_count)
        if state.current_file_path:
            stats_counter.set("archiver.current_file", str(state.current_file_path.name))

        # Check if we were in error state and reset to running
        service_info = await status_manager.get_service_info("archiver")
        if service_info and service_info.status == ServiceStatus.ERROR:
            logger.info("Service recovered from error state, resetting to RUNNING")
            await status_manager.update_service_status("archiver", ServiceStatus.RUNNING)

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        stats_counter.increment("archiver.errors")
        await status_manager.update_service_status(
            "archiver", ServiceStatus.ERROR, f"Error processing message: {e}"
        )
        raise
