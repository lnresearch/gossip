import asyncio
import logging
import sqlite3
from typing import Optional
import aio_pika
from aio_pika import Message, ExchangeType
from ..config import Config
from ..models import create_database
from ..status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


class DedupService:
    """Service that deduplicates gossip messages using SQLite."""
    
    def __init__(self, config: Config):
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.raw_exchange: Optional[aio_pika.Exchange] = None
        self.uniq_exchange: Optional[aio_pika.Exchange] = None
        self.running = False
        
        # Initialize database
        create_database(config.database_path)
        
    async def start(self) -> None:
        """Start the dedup service."""
        logger.info("Starting dedup service")
        await status_manager.update_service_status("dedup", ServiceStatus.STARTING)
        
        try:
            # Connect to RabbitMQ
            self.connection = await aio_pika.connect_robust(self.config.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # Declare exchanges
            self.raw_exchange = await self.channel.declare_exchange(
                "lnr.gossip.raw",
                ExchangeType.DIRECT,
                durable=True
            )
            
            self.uniq_exchange = await self.channel.declare_exchange(
                "lnr.gossip.uniq", 
                ExchangeType.FANOUT,
                durable=True
            )
            
            # Declare queue for consuming raw messages
            raw_queue = await self.channel.declare_queue(
                "lnr.gossip.dedup",
                durable=True
            )
            
            # Bind queue to raw exchange
            await raw_queue.bind(self.raw_exchange)
            
            # Start consuming
            await raw_queue.consume(self._process_message)
            
            self.running = True
            await status_manager.update_service_status("dedup", ServiceStatus.RUNNING)
            logger.info("Dedup service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start dedup service: {e}")
            await status_manager.update_service_status(
                "dedup",
                ServiceStatus.ERROR,
                str(e)
            )
            raise
    
    async def _process_message(self, message: aio_pika.IncomingMessage) -> None:
        """Process a raw message and check for duplicates."""
        async with message.process():
            try:
                raw_data = message.body
                
                # Validate message starts with Lightning Network gossip message types
                if not self._is_valid_gossip_message(raw_data):
                    logger.debug(f"Ignoring non-gossip message of length {len(raw_data)}")
                    return
                
                # Strip protobuf envelope if present
                processed_data = self._strip_protobuf_envelope(raw_data)
                
                # Check if message is unique
                if await self._is_unique_message(processed_data):
                    # Forward unique message to uniq exchange
                    uniq_message = Message(
                        processed_data,
                        headers=message.headers or {},
                        timestamp=message.timestamp,
                    )
                    
                    await self.uniq_exchange.publish(uniq_message, routing_key="")
                    await status_manager.increment_message_count("dedup")
                    logger.debug(f"Forwarded unique message of {len(processed_data)} bytes")
                else:
                    logger.debug(f"Dropped duplicate message of {len(processed_data)} bytes")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await status_manager.update_service_status(
                    "dedup",
                    ServiceStatus.ERROR,
                    f"Error processing message: {e}"
                )
    
    def _is_valid_gossip_message(self, data: bytes) -> bool:
        """Check if message starts with valid Lightning Network gossip message types."""
        if len(data) < 2:
            return False
        
        # Lightning Network gossip message types
        # 0x0100 = channel_announcement
        # 0x0101 = node_announcement  
        # 0x0102 = channel_update
        valid_types = [b'\x01\x00', b'\x01\x01', b'\x01\x02']
        
        return data[:2] in valid_types
    
    def _strip_protobuf_envelope(self, data: bytes) -> bytes:
        """Strip protobuf envelope if present, otherwise return original data."""
        # This is a placeholder - in a real implementation, you would
        # detect and strip protobuf envelopes based on the actual format
        # For now, we'll just return the original data
        return data
    
    async def _is_unique_message(self, data: bytes) -> bool:
        """Check if message is unique and store it if it is."""
        # Run database operations in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._check_and_store_message, data)
    
    def _check_and_store_message(self, data: bytes) -> bool:
        """Check if message is unique in SQLite database and store if new."""
        conn = sqlite3.connect(self.config.database_path)
        try:
            # Try to insert the message
            conn.execute("INSERT INTO messages (raw) VALUES (?)", (data,))
            conn.commit()
            return True  # Message was unique and stored
        except sqlite3.IntegrityError:
            # Message already exists
            return False
        finally:
            conn.close()
    
    async def stop(self) -> None:
        """Stop the dedup service."""
        logger.info("Stopping dedup service")
        self.running = False
        
        if self.connection:
            await self.connection.close()
            
        await status_manager.update_service_status("dedup", ServiceStatus.STOPPED)
        logger.info("Dedup service stopped")
    
    async def run(self) -> None:
        """Run the dedup service."""
        await self.start()
        
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()
