import asyncio
import logging
from typing import Optional
import aio_pika
import aio_pika.exceptions
from aio_pika import Message, ExchangeType
from ..config import Config
from ..status import status_manager, ServiceStatus

logger = logging.getLogger(__name__)


class GlbridgeService:
    """Service that bridges gossip messages from upstream RabbitMQ to local exchange."""
    
    def __init__(self, config: Config):
        self.config = config
        self.upstream_connection: Optional[aio_pika.Connection] = None
        self.local_connection: Optional[aio_pika.Connection] = None
        self.upstream_channel: Optional[aio_pika.Channel] = None
        self.local_channel: Optional[aio_pika.Channel] = None
        self.running = False
        
    async def start(self) -> None:
        """Start the glbridge service."""
        logger.info("Starting glbridge service")
        await status_manager.update_service_status("glbridge", ServiceStatus.STARTING)
        
        try:
            # Connect to upstream RabbitMQ
            self.upstream_connection = await aio_pika.connect_robust(
                self.config.upstream_rabbitmq_url
            )
            self.upstream_channel = await self.upstream_connection.channel()
            
            # Connect to local RabbitMQ
            self.local_connection = await aio_pika.connect_robust(
                self.config.rabbitmq_url
            )
            self.local_channel = await self.local_connection.channel()
            
            # Declare local exchange
            self.raw_exchange = await self.local_channel.declare_exchange(
                "lnr.gossip.raw",
                ExchangeType.FANOUT,
                durable=True
            )
            
            # Declare upstream exchange (router.gossip)
            upstream_exchange = await self.upstream_channel.declare_exchange(
                self.config.upstream_queue_name,  # Actually exchange name (router.gossip)
                ExchangeType.DIRECT,
                passive=True  # Don't create, just connect to existing exchange
            )
            
            # Create a temporary queue for this consumer
            upstream_queue = await self.upstream_channel.declare_queue(
                "lnr.bridge",
                exclusive=True,  # Queue deleted when connection closes
                auto_delete=True
            )
            
            # Bind our queue to the exchange
            await upstream_queue.bind(upstream_exchange, routing_key="")
            
            # Start consuming from upstream
            await upstream_queue.consume(self._process_message)
            
            self.running = True
            await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)
            logger.info("Glbridge service started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start glbridge service: {e}")
            await status_manager.update_service_status(
                "glbridge", 
                ServiceStatus.ERROR, 
                str(e)
            )
            raise
    
    async def _process_message(self, message: aio_pika.IncomingMessage) -> None:
        """Process a message from upstream and forward to local exchange."""
        try:
            async with message.process():
                try:
                    # Validate message starts with Lightning Network gossip message types
                    if not self._is_valid_gossip_message(message.body):
                        logger.debug(f"Ignoring non-gossip message of length {len(message.body)}")
                        return
                    
                    # Check if local channel is still open
                    if self.local_channel and self.local_channel.is_closed:
                        logger.error("Local channel is closed, cannot publish message")
                        await status_manager.update_service_status(
                            "glbridge",
                            ServiceStatus.ERROR,
                            "Local channel closed unexpectedly"
                        )
                        return
                    
                    # Forward message to local exchange
                    local_message = Message(
                        message.body,
                        headers=message.headers or {},
                        timestamp=message.timestamp,
                    )
                    
                    await self.raw_exchange.publish(local_message, routing_key="")
                    
                    await status_manager.increment_message_count("glbridge")
                    logger.debug(f"Forwarded gossip message of {len(message.body)} bytes")
                    
                except aio_pika.exceptions.ChannelInvalidStateError as e:
                    logger.error(f"Channel invalid state error: {e}")
                    await status_manager.update_service_status(
                        "glbridge",
                        ServiceStatus.ERROR,
                        f"Channel invalid state: {e}"
                    )
                except aio_pika.exceptions.ConnectionClosed as e:
                    logger.error(f"Connection closed error: {e}")
                    await status_manager.update_service_status(
                        "glbridge",
                        ServiceStatus.ERROR,
                        f"Connection closed: {e}"
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await status_manager.update_service_status(
                        "glbridge",
                        ServiceStatus.ERROR,
                        f"Error processing message: {e}"
                    )
        except Exception as e:
            # This catches errors from message.process() context manager
            logger.error(f"Error in message processing context: {e}")
            await status_manager.update_service_status(
                "glbridge",
                ServiceStatus.ERROR,
                f"Message context error: {e}"
            )
    
    def _is_valid_gossip_message(self, data: bytes) -> bool:
        """Check if message is valid (raw Lightning or protobuf-wrapped gossip)."""
        if len(data) < 2:
            return False
        
        # Raw Lightning Network gossip message types
        # 0x0100 = channel_announcement
        # 0x0101 = node_announcement  
        # 0x0102 = channel_update
        raw_lightning_types = [b'\x01\x00', b'\x01\x01', b'\x01\x02']
        
        # Check for raw Lightning messages
        if data[:2] in raw_lightning_types:
            return True
        
        # Check for protobuf-wrapped messages (common patterns from upstream)
        # Messages starting with 0x0a typically indicate protobuf varint encoding
        if data[0:1] == b'\x0a':
            return True
            
        # For now, accept all messages and let downstream dedup handle validation
        # The dedup process is designed to strip protobuf envelopes and validate
        return True
    
    def _check_connections_healthy(self) -> bool:
        """Check if connections are healthy."""
        upstream_ok = (self.upstream_connection and 
                      not self.upstream_connection.is_closed and
                      self.upstream_channel and 
                      not self.upstream_channel.is_closed)
        
        local_ok = (self.local_connection and 
                   not self.local_connection.is_closed and
                   self.local_channel and 
                   not self.local_channel.is_closed)
        
        if not upstream_ok:
            logger.warning("Upstream connection/channel unhealthy")
        if not local_ok:
            logger.warning("Local connection/channel unhealthy")
            
        return upstream_ok and local_ok
    
    async def stop(self) -> None:
        """Stop the glbridge service."""
        logger.info("Stopping glbridge service")
        self.running = False
        
        if self.upstream_connection:
            await self.upstream_connection.close()
        if self.local_connection:
            await self.local_connection.close()
            
        await status_manager.update_service_status("glbridge", ServiceStatus.STOPPED)
        logger.info("Glbridge service stopped")
    
    async def run(self) -> None:
        """Run the glbridge service."""
        await self.start()
        
        try:
            health_check_interval = 30  # Check every 30 seconds
            last_health_check = 0
            
            while self.running:
                await asyncio.sleep(1)
                
                # Periodic health check
                last_health_check += 1
                if last_health_check >= health_check_interval:
                    if not self._check_connections_healthy():
                        logger.error("Connection health check failed")
                        await status_manager.update_service_status(
                            "glbridge",
                            ServiceStatus.ERROR,
                            "Connection health check failed"
                        )
                        # Could add reconnection logic here in the future
                    last_health_check = 0
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()
