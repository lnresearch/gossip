import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TextIO
import aio_pika
from aio_pika import ExchangeType
from ..config import Config
from ..status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


class ArchiverService:
    """Service that archives unique gossip messages to daily/hourly snapshot files."""
    
    def __init__(self, config: Config):
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.uniq_exchange: Optional[aio_pika.Exchange] = None
        self.current_file: Optional[TextIO] = None
        self.current_file_path: Optional[Path] = None
        self.last_rotation_time: Optional[datetime] = None
        self.running = False
        
        # Ensure archive directory exists
        Path(config.archive_directory).mkdir(parents=True, exist_ok=True)
        
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
                if self.current_file:
                    # Write as hex-encoded message with timestamp
                    timestamp = datetime.now(timezone.utc).isoformat()
                    hex_data = message.body.hex()
                    line = f"{timestamp},{hex_data}\n"
                    
                    # Run file operation in thread pool to avoid blocking
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self._write_to_file, line)
                    
                    await status_manager.increment_message_count("archiver")
                    logger.debug(f"Archived message of {len(message.body)} bytes")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await status_manager.update_service_status(
                    "archiver",
                    ServiceStatus.ERROR,
                    f"Error processing message: {e}"
                )
    
    def _write_to_file(self, line: str) -> None:
        """Write line to current file (blocking operation)."""
        if self.current_file:
            self.current_file.write(line)
            self.current_file.flush()
    
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
        # Close current file if open
        if self.current_file:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
            logger.info(f"Closed archive file: {self.current_file_path}")
        
        # Generate new filename
        if self.config.archive_rotation == "hourly":
            filename = f"gossip_{timestamp.strftime('%Y%m%d_%H')}.csv"
        else:  # daily
            filename = f"gossip_{timestamp.strftime('%Y%m%d')}.csv"
        
        self.current_file_path = Path(self.config.archive_directory) / filename
        
        # Open new file
        loop = asyncio.get_event_loop()
        self.current_file = await loop.run_in_executor(
            None, 
            lambda: open(self.current_file_path, 'a', encoding='utf-8')
        )
        
        self.last_rotation_time = timestamp
        logger.info(f"Opened new archive file: {self.current_file_path}")
        
        # Write header if file is empty
        if self.current_file_path.stat().st_size == 0:
            await loop.run_in_executor(
                None,
                lambda: self.current_file.write("timestamp,hex_data\n")
            )
    
    async def stop(self) -> None:
        """Stop the archiver service."""
        logger.info("Stopping archiver service")
        self.running = False
        
        # Close current file
        if self.current_file:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.current_file.close)
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