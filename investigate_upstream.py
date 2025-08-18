#!/usr/bin/env python3
"""
Test script to investigate upstream RabbitMQ server structure.
Connects to the upstream server and lists exchanges, queues, and bindings.
"""

import asyncio
import logging
import aio_pika
from aio_pika import ExchangeType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

UPSTREAM_URL = "amqp://greenlight:23b3469aa4fe8757001c@10.64.4.6/prod"


async def investigate_upstream():
    """Connect to upstream server and investigate its structure."""
    try:
        logger.info(f"Connecting to upstream server: {UPSTREAM_URL}")
        connection = await aio_pika.connect_robust(UPSTREAM_URL)
        channel = await connection.channel()
        
        logger.info("✅ Successfully connected to upstream server!")
        
        # Try to investigate what exchanges exist
        logger.info("\n=== Investigating Exchanges ===")
        
        # Check for router.gossip exchange
        logger.info("🔍 Checking for router.gossip exchange...")
        try:
            exchange = await channel.declare_exchange(
                "router.gossip",
                ExchangeType.DIRECT,
                passive=True  # Only check if it exists, don't create
            )
            logger.info("✅ Exchange 'router.gossip' exists (type: DIRECT)")
        except Exception as e:
            logger.info(f"❌ Exchange 'router.gossip' (DIRECT) does not exist: {e}")
        
        # Try other exchange types for router.gossip
        for exchange_type in [ExchangeType.FANOUT, ExchangeType.TOPIC, ExchangeType.HEADERS]:
            try:
                exchange = await channel.declare_exchange(
                    "router.gossip",
                    exchange_type,
                    passive=True
                )
                logger.info(f"✅ Exchange 'router.gossip' exists (type: {exchange_type.value})")
                break
            except Exception:
                continue
        
        # Try to declare the gossip.raw exchange in passive mode to see if it exists
        try:
            exchange = await channel.declare_exchange(
                "gossip.raw",
                ExchangeType.DIRECT,
                passive=True  # Only check if it exists, don't create
            )
            logger.info("✅ Exchange 'gossip.raw' exists (type: DIRECT)")
        except Exception as e:
            logger.info(f"❌ Exchange 'gossip.raw' (DIRECT) does not exist: {e}")
        
        # Try other exchange types
        for exchange_type in [ExchangeType.FANOUT, ExchangeType.TOPIC, ExchangeType.HEADERS]:
            try:
                exchange = await channel.declare_exchange(
                    "gossip.raw",
                    exchange_type,
                    passive=True
                )
                logger.info(f"✅ Exchange 'gossip.raw' exists (type: {exchange_type.value})")
                break
            except Exception:
                continue
        
        # Try to investigate if there's a queue named gossip.raw
        logger.info("\n=== Investigating Queues ===")
        try:
            queue = await channel.declare_queue(
                "gossip.raw",
                passive=True  # Only check if it exists, don't create
            )
            logger.info("✅ Queue 'gossip.raw' exists")
            logger.info(f"   - Message count: {queue.declaration_result.message_count}")
            logger.info(f"   - Consumer count: {queue.declaration_result.consumer_count}")
        except Exception as e:
            logger.info(f"❌ Queue 'gossip.raw' does not exist: {e}")
        
        # Try a few variations of common queue/exchange names
        logger.info("\n=== Checking Common Variations ===")
        variations = [
            "gossip",
            "raw", 
            "lnr.gossip.raw",
            "greenlight.gossip.raw",
            "prod.gossip.raw",
            "amq.direct",
            "amq.fanout", 
            "amq.topic",
            "amq.headers",
            "",  # default exchange
            "lnresearch",
            "lightning",
            "network",
            "messages",
            "gossip_exchange",
            "gossip-exchange", 
            "gl.gossip",
            "gl.gossip.raw",
            "greenlight",
            "gl",
            "lnd",
            "lnd.gossip",
            "cln", 
            "cln.gossip",
            "btc",
            "bitcoin"
        ]
        
        logger.info("--- Exchanges ---")
        for name in variations:
            for exchange_type in [ExchangeType.DIRECT, ExchangeType.FANOUT, ExchangeType.TOPIC, ExchangeType.HEADERS]:
                try:
                    await channel.declare_exchange(name, exchange_type, passive=True)
                    logger.info(f"✅ Found exchange: '{name}' (type: {exchange_type.value})")
                except:
                    pass
        
        logger.info("--- Queues ---")
        for name in variations:
            try:
                queue = await channel.declare_queue(name, passive=True)
                logger.info(f"✅ Found queue: '{name}' (messages: {queue.declaration_result.message_count})")
            except:
                pass
        
        # Try to test a temporary subscription to see what we can consume
        logger.info("\n=== Testing Temporary Consumer ===")
        try:
            # Create a temporary queue
            temp_queue = await channel.declare_queue("", exclusive=True, auto_delete=True)
            logger.info(f"✅ Created temporary queue: {temp_queue.name}")
            
            # Try to bind it to router.gossip exchange
            try:
                await temp_queue.bind("router.gossip", routing_key="")
                logger.info("✅ Successfully bound to 'router.gossip' exchange")
                
                # Set up a temporary consumer to see if messages flow
                message_count = 0
                
                async def test_consumer(message: aio_pika.IncomingMessage):
                    nonlocal message_count
                    async with message.process():
                        message_count += 1
                        logger.info(f"📨 Received message #{message_count}: {len(message.body)} bytes")
                        # Log first few bytes as hex for gossip message identification
                        if len(message.body) >= 2:
                            hex_start = message.body[:2].hex()
                            logger.info(f"   Message starts with: 0x{hex_start}")
                        
                        if message_count >= 5:  # Stop after 5 messages
                            return
                
                await temp_queue.consume(test_consumer)
                logger.info("🔄 Listening for messages for 10 seconds...")
                await asyncio.sleep(10)
                
                if message_count == 0:
                    logger.info("⚠️  No messages received during test period")
                else:
                    logger.info(f"✅ Received {message_count} messages during test")
                
            except Exception as e:
                logger.info(f"❌ Failed to bind to router.gossip exchange: {e}")
                
        except Exception as e:
            logger.info(f"❌ Failed to create temporary consumer: {e}")
        
        await connection.close()
        logger.info("\n🔌 Disconnected from upstream server")
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to upstream server: {e}")
        return


if __name__ == "__main__":
    asyncio.run(investigate_upstream())