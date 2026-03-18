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
        logger.info("=" * 60)
        logger.info("Configuration Debug Information:")
        logger.info(f"  Upstream RabbitMQ URL: {self.config.upstream_rabbitmq_url}")
        logger.info(f"  Local RabbitMQ URL: {self.config.rabbitmq_url}")
        logger.info(f"  Upstream Exchange Name: {self.config.upstream_queue_name}")
        logger.info("=" * 60)
        
        await status_manager.update_service_status("glbridge", ServiceStatus.STARTING)
        
        try:
            # Connect to upstream RabbitMQ
            logger.info("Attempting to connect to upstream RabbitMQ...")
            try:
                self.upstream_connection = await aio_pika.connect_robust(
                    self.config.upstream_rabbitmq_url
                )
                logger.info("✅ Successfully connected to upstream RabbitMQ")
                logger.info(f"  Connection state: {'OPEN' if not self.upstream_connection.is_closed else 'CLOSED'}")
                logger.info(f"  Server properties: {getattr(self.upstream_connection, 'server_properties', 'N/A')}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to upstream RabbitMQ: {e}")
                logger.error(f"  URL attempted: {self.config.upstream_rabbitmq_url}")
                raise

            logger.info("Creating upstream channel...")
            try:
                self.upstream_channel = await self.upstream_connection.channel()
                logger.info("✅ Successfully created upstream channel")
                logger.info(f"  Channel ID: {self.upstream_channel.number}")
                logger.info(f"  Channel state: {'OPEN' if not self.upstream_channel.is_closed else 'CLOSED'}")
            except Exception as e:
                logger.error(f"❌ Failed to create upstream channel: {e}")
                raise
            
            # Connect to local RabbitMQ
            logger.info("Attempting to connect to local RabbitMQ...")
            try:
                self.local_connection = await aio_pika.connect_robust(
                    self.config.rabbitmq_url
                )
                logger.info("✅ Successfully connected to local RabbitMQ")
                logger.info(f"  Connection state: {'OPEN' if not self.local_connection.is_closed else 'CLOSED'}")
                logger.info(f"  Server properties: {getattr(self.local_connection, 'server_properties', 'N/A')}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to local RabbitMQ: {e}")
                logger.error(f"  URL attempted: {self.config.rabbitmq_url}")
                raise

            logger.info("Creating local channel...")
            try:
                self.local_channel = await self.local_connection.channel()
                logger.info("✅ Successfully created local channel")
                logger.info(f"  Channel ID: {self.local_channel.number}")
                logger.info(f"  Channel state: {'OPEN' if not self.local_channel.is_closed else 'CLOSED'}")
            except Exception as e:
                logger.error(f"❌ Failed to create local channel: {e}")
                raise
            
            # Declare local exchange
            logger.info("Declaring local exchange...")
            try:
                self.raw_exchange = await self.local_channel.declare_exchange(
                    "lnr.gossip.raw",
                    ExchangeType.FANOUT,  # Back to FANOUT as originally designed
                    durable=True
                )
                logger.info("✅ Successfully declared local exchange 'lnr.gossip.raw'")
                logger.info(f"  Exchange type: FANOUT")
                logger.info(f"  Exchange durable: True")
            except Exception as e:
                logger.error(f"❌ Failed to declare local exchange: {e}")
                raise
            
            # Connect to upstream exchange (assuming it's a FANOUT exchange)
            logger.info(f"Attempting to connect to upstream exchange '{self.config.upstream_queue_name}'...")
            try:
                upstream_exchange = await self.upstream_channel.declare_exchange(
                    self.config.upstream_queue_name,  # Actually exchange name (router.gossip)
                    ExchangeType.FANOUT,  # Assume FANOUT - controlled by producers
                    passive=True  # Don't create, just connect to existing exchange
                )
                logger.info(f"✅ Successfully connected to upstream exchange '{self.config.upstream_queue_name}'")
                logger.info(f"  Exchange type: FANOUT")
                logger.info(f"  Passive connection: True (exchange must exist)")
            except Exception as e:
                logger.error(f"❌ Failed to connect to upstream exchange '{self.config.upstream_queue_name}': {e}")
                logger.error("  This usually means the exchange doesn't exist or has a different type")
                raise
            
            # Create a durable queue with single-active-consumer for this consumer
            # Using durable + x-single-active-consumer instead of exclusive to avoid
            # RESOURCE_LOCKED errors on reconnection (see AIKB: gossip/glbridge-exclusive-queue-lock)
            logger.info("Creating consumer queue...")
            try:
                upstream_queue = await self.upstream_channel.declare_queue(
                    "lnr.bridge",
                    durable=True,
                    arguments={"x-single-active-consumer": True}
                )
                logger.info("✅ Successfully created consumer queue")
                logger.info(f"  Queue name: {upstream_queue.name}")
                logger.info(f"  Queue durable: True")
                logger.info(f"  Queue x-single-active-consumer: True")
            except Exception as e:
                logger.error(f"❌ Failed to create consumer queue: {e}")
                raise
            
            # Bind our queue to the FANOUT exchange (no routing key needed)
            logger.info("Binding queue to upstream exchange...")
            try:
                await upstream_queue.bind(upstream_exchange)
                logger.info(f"✅ Successfully bound queue to upstream FANOUT exchange '{self.config.upstream_queue_name}'")
                logger.info("  No routing key needed for FANOUT exchange")
            except Exception as e:
                logger.error(f"❌ Failed to bind queue to exchange: {e}")
                raise
            
            # Start consuming from upstream
            logger.info("Starting message consumer...")
            try:
                await upstream_queue.consume(self._process_message)
                logger.info("✅ Successfully started consuming messages from upstream")
                logger.info(f"  Consumer callback: {self._process_message.__name__}")
            except Exception as e:
                logger.error(f"❌ Failed to start consuming messages: {e}")
                raise
            
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

                    # Check if we were in error state and reset to running
                    service_info = await status_manager.get_service_info("glbridge")
                    if service_info and service_info.status == ServiceStatus.ERROR:
                        logger.info("Service recovered from error state, resetting to RUNNING")
                        await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)

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
                    else:
                        # Health check passed - ensure we're in RUNNING state
                        service_info = await status_manager.get_service_info("glbridge")
                        if service_info and service_info.status == ServiceStatus.ERROR:
                            logger.info("Health check passed, service recovered from error state")
                            await status_manager.update_service_status("glbridge", ServiceStatus.RUNNING)
                    last_health_check = 0
                    
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()
