import asyncio
import logging
import subprocess
import shutil
import bz2
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, BinaryIO
import aio_pika
from aio_pika import ExchangeType
from ..config import Config
from ..status import status_manager, ServiceStatus
from ..gsp import GSPWriter

logger = logging.getLogger(__name__)


class ArchiverService:
    """Service that archives unique gossip messages to daily/hourly snapshot files."""
    
    def __init__(self, config: Config):
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.uniq_exchange: Optional[aio_pika.Exchange] = None
        self.current_file: Optional[BinaryIO] = None
        self.gsp_writer: Optional[GSPWriter] = None
        self.current_file_path: Optional[Path] = None
        self.last_rotation_time: Optional[datetime] = None
        self.running = False
        
        # Track current archive statistics
        self.current_archive_message_count = 0
        self.current_archive_byte_count = 0
        
        # Ensure directories exist
        Path(config.archive_temp_directory).mkdir(parents=True, exist_ok=True)
        Path(config.archive_directory).mkdir(parents=True, exist_ok=True)
        Path(config.git_annex_directory).mkdir(parents=True, exist_ok=True)
    
    def calculate_next_flush_time(self) -> Optional[datetime]:
        """Calculate the time of the next archive file rotation."""
        if not self.last_rotation_time:
            return None
            
        now = datetime.now(timezone.utc)
        
        if self.config.archive_rotation == "hourly":
            # Next flush at the top of the next hour
            next_hour = now.replace(minute=0, second=0, microsecond=0)
            if next_hour <= now:
                next_hour = next_hour.replace(hour=next_hour.hour + 1)
                if next_hour.hour == 24:
                    next_hour = next_hour.replace(hour=0)
                    next_hour = next_hour.replace(day=next_hour.day + 1)
            return next_hour
        else:  # daily
            # Next flush at midnight of the next day
            next_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if next_day <= now:
                next_day = next_day.replace(day=next_day.day + 1)
            return next_day
    
    @staticmethod
    def format_bytes(bytes_count: int) -> str:
        """Format bytes into human readable form."""
        if bytes_count < 1024:
            return f"{bytes_count} B"
        elif bytes_count < 1024 * 1024:
            return f"{bytes_count / 1024:.1f} KB"
        elif bytes_count < 1024 * 1024 * 1024:
            return f"{bytes_count / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_count / (1024 * 1024 * 1024):.1f} GB"
        
    async def start(self) -> None:
        """Start the archiver service."""
        logger.info("Starting archiver service")
        await status_manager.update_service_status("archiver", ServiceStatus.STARTING)
        
        try:
            # Connect to RabbitMQ
            self.connection = await aio_pika.connect_robust(self.config.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # Declare uniq exchange
            self.uniq_exchange = await self.channel.declare_exchange(
                "lnr.gossip.uniq",
                ExchangeType.FANOUT,
                durable=True
            )
            
            # Declare queue for consuming unique messages
            archive_queue = await self.channel.declare_queue(
                "lnr.gossip.archiver",
                durable=True
            )
            
            # Bind queue to uniq exchange
            await archive_queue.bind(self.uniq_exchange)
            
            # Initialize first archive file
            await self._rotate_file_if_needed()
            
            # Start consuming
            await archive_queue.consume(self._process_message)
            
            self.running = True
            await status_manager.update_service_status("archiver", ServiceStatus.RUNNING)
            logger.info("Archiver service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start archiver service: {e}")
            await status_manager.update_service_status(
                "archiver",
                ServiceStatus.ERROR,
                str(e)
            )
            raise
    
    async def _process_message(self, message: aio_pika.IncomingMessage) -> None:
        """Process a unique message and write to archive file."""
        async with message.process():
            try:
                # Check if we need to rotate the file
                await self._rotate_file_if_needed()
                
                # Write message to current archive file
                if self.gsp_writer:
                    # Run GSP write operation in thread pool to avoid blocking
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self._write_gsp_message, message.body)
                    
                    # Update statistics (approximate bytes from CompactSize + message)
                    message_size = len(message.body)
                    compact_size_bytes = len(self._encode_compact_size_sync(message_size))
                    total_bytes = compact_size_bytes + message_size
                    
                    self.current_archive_message_count += 1
                    self.current_archive_byte_count += total_bytes
                    
                    # Update status manager
                    await status_manager.increment_message_count("archiver")
                    await status_manager.update_archiver_stats(
                        self.current_archive_message_count,
                        self.current_archive_byte_count
                    )
                    
                    logger.debug(f"Archived message of {len(message.body)} bytes")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await status_manager.update_service_status(
                    "archiver",
                    ServiceStatus.ERROR,
                    f"Error processing message: {e}"
                )
    
    def _write_gsp_message(self, message_data: bytes) -> None:
        """Write GSP message to current file (blocking operation)."""
        if self.gsp_writer:
            self.gsp_writer.write_message(message_data)
            self.gsp_writer.flush()
    
    def _encode_compact_size_sync(self, value: int) -> bytes:
        """Synchronous helper to encode CompactSize for byte counting."""
        from ..gsp import encode_compact_size
        return encode_compact_size(value)
    
    async def _rotate_file_if_needed(self) -> None:
        """Rotate archive file if needed based on time."""
        now = datetime.now(timezone.utc)
        
        should_rotate = False
        
        if self.last_rotation_time is None:
            should_rotate = True
        elif self.config.archive_rotation == "hourly":
            # Rotate every hour
            should_rotate = (now.hour != self.last_rotation_time.hour or
                           now.date() != self.last_rotation_time.date())
        else:  # daily
            # Rotate every day
            should_rotate = now.date() != self.last_rotation_time.date()
        
        if should_rotate:
            await self._rotate_file(now)
    
    async def _rotate_file(self, timestamp: datetime) -> None:
        """Rotate to a new archive file."""
        old_file_path = None

        # Close and process current file if open
        if self.current_file:
            old_file_path = self.current_file_path
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
            self.gsp_writer = None
            logger.info(f"Closed archive file: {old_file_path}")

            # Upload old file to GCS and add to git-annex
            if old_file_path and old_file_path.exists():
                await self._upload_and_annex_file(old_file_path)

        # Generate new filename
        if self.config.archive_rotation == "hourly":
            filename = f"gossip-{timestamp.strftime('%Y%m%d%H%M')}.gsp"
        else:  # daily
            filename = f"gossip-{timestamp.strftime('%Y%m%d')}0000.gsp"

        # Use temp directory for active file
        self.current_file_path = Path(self.config.archive_temp_directory) / filename

        # Open new file in binary mode
        loop = asyncio.get_event_loop()
        self.current_file = await loop.run_in_executor(
            None,
            lambda: open(self.current_file_path, 'ab')
        )

        # Create GSP writer
        self.gsp_writer = GSPWriter(self.current_file)

        self.last_rotation_time = timestamp
        logger.info(f"Opened new archive file: {self.current_file_path}")

        # Reset counters for new file
        self.current_archive_message_count = 0
        self.current_archive_byte_count = 0

        # GSP header will be written automatically on first message
        # Add header size to byte count if file is new
        if self.current_file_path.stat().st_size == 0:
            from ..gsp import GSP_HEADER
            self.current_archive_byte_count = len(GSP_HEADER)

        # Update status manager with reset values
        await status_manager.update_archiver_stats(
            self.current_archive_message_count,
            self.current_archive_byte_count
        )

    async def _upload_and_annex_file(self, file_path: Path) -> None:
        """Compress, upload file to GCS and add to git-annex."""
        compressed_path = None
        try:
            # Compress file with bzip2
            loop = asyncio.get_event_loop()
            compressed_path = await loop.run_in_executor(
                None,
                self._compress_file,
                file_path
            )

            compressed_filename = compressed_path.name
            gcs_url = f"{self.config.gcs_bucket_url.rstrip('/')}/{compressed_filename}"

            logger.info(f"Uploading compressed file {compressed_path} to GCS: {gcs_url}")

            # Upload compressed file to GCS using Python client
            await loop.run_in_executor(
                None,
                self._upload_to_gcs,
                str(compressed_path),
                compressed_filename
            )

            # Add to git-annex
            await loop.run_in_executor(
                None,
                self._add_to_git_annex,
                compressed_filename,
                gcs_url
            )

            # Remove temp files
            file_path.unlink()
            compressed_path.unlink()
            logger.info(f"Removed temporary files: {file_path}, {compressed_path}")

        except Exception as e:
            logger.error(f"Failed to upload and annex file {file_path}: {e}")

            # Clean up compressed file if it exists
            if compressed_path and compressed_path.exists():
                compressed_path.unlink()

            # Move original file to archive directory as fallback
            fallback_path = Path(self.config.archive_directory) / file_path.name
            shutil.move(str(file_path), str(fallback_path))
            logger.info(f"Moved file to fallback location: {fallback_path}")

    def _compress_file(self, file_path: Path) -> Path:
        """Compress file with bzip2 (blocking operation)."""
        try:
            compressed_path = file_path.with_suffix(file_path.suffix + '.bz2')

            logger.info(f"Compressing {file_path} to {compressed_path}")

            with open(file_path, 'rb') as f_in:
                with bz2.BZ2File(compressed_path, 'wb', compresslevel=9) as f_out:
                    shutil.copyfileobj(f_in, f_out)

            original_size = file_path.stat().st_size
            compressed_size = compressed_path.stat().st_size
            compression_ratio = compressed_size / original_size if original_size > 0 else 1.0

            logger.info(f"Compression completed: {original_size} -> {compressed_size} bytes "
                       f"(ratio: {compression_ratio:.2f})")

            return compressed_path

        except Exception as e:
            logger.error(f"Failed to compress file {file_path}: {e}")
            raise

    def _upload_to_gcs(self, file_path: str, filename: str) -> None:
        """Upload file to GCS using Python client (blocking operation)."""
        try:
            from google.cloud import storage

            # Extract bucket name from URL
            bucket_name = "lnresearch"
            blob_name = f"daily/{filename}"

            # Initialize client and upload
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            with open(file_path, 'rb') as f:
                blob.upload_from_file(f)

            logger.info(f"Successfully uploaded {file_path} to gs://{bucket_name}/{blob_name}")

        except ImportError:
            logger.error("google-cloud-storage not installed. Please install it: pip install google-cloud-storage")
            raise
        except Exception as e:
            logger.error(f"GCS upload failed: {e}")
            raise

    def _add_to_git_annex(self, filename: str, gcs_url: str) -> None:
        """Add file URL to git-annex (blocking operation)."""
        try:
            annex_dir = Path(self.config.git_annex_directory)

            # Change to annex directory
            original_cwd = Path.cwd()
            import os
            os.chdir(annex_dir)

            try:
                # Add URL to git-annex
                result = subprocess.run(
                    ["git", "annex", "addurl", gcs_url, "--file", filename],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Added {filename} to git-annex with URL {gcs_url}")

                # Commit the change
                subprocess.run(
                    ["git", "add", filename],
                    capture_output=True,
                    text=True,
                    check=True
                )

                commit_msg = f"Add gossip archive {filename}\n\nAutomatic archive upload from lnr archiver service"
                subprocess.run(
                    ["git", "commit", "-m", commit_msg],
                    capture_output=True,
                    text=True,
                    check=True
                )

                # Push to GitHub
                subprocess.run(
                    ["git", "push", self.config.github_remote],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Committed and pushed {filename} to git-annex")

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"git-annex operation failed: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"git-annex operation failed: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the archiver service."""
        logger.info("Stopping archiver service")
        self.running = False
        
        # Close current file and GSP writer
        if self.current_file:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
            self.gsp_writer = None
            logger.info(f"Closed archive file: {self.current_file_path}")
        
        if self.connection:
            await self.connection.close()
            
        await status_manager.update_service_status("archiver", ServiceStatus.STOPPED)
        logger.info("Archiver service stopped")
    
    async def run(self) -> None:
        """Run the archiver service."""
        await self.start()
        
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()