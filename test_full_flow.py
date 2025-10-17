#!/usr/bin/env python3
"""
Test the full message flow: raw exchange -> dedup service -> uniq exchange.
"""

import asyncio
import logging
import sys
import os

# Add the project to Python path
sys.path.insert(0, os.path.dirname(__file__))

import aio_pika
from aio_pika import ExchangeType, Message
from lnr.config import Config
from lnr.services.dedup import DedupService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

LOCAL_URL = "amqp://guest:guest@localhost:35672/"


async def test_full_flow():
    """Test the complete message flow through dedup service."""
    try:
        # Connect to RabbitMQ for publishing and monitoring
        logger.info("Setting up test environment...")
        connection = await aio_pika.connect_robust(LOCAL_URL)
        channel = await connection.channel()
        
        # Declare exchanges
        raw_exchange = await channel.declare_exchange(
            "lnr.gossip.raw", ExchangeType.DIRECT, durable=True
        )
        uniq_exchange = await channel.declare_exchange(
            "lnr.gossip.uniq", ExchangeType.FANOUT, durable=True
        )
        
        # Create monitor queue for uniq messages
        monitor_queue = await channel.declare_queue(
            "test.uniq.monitor", exclusive=True, auto_delete=True
        )
        await monitor_queue.bind(uniq_exchange)
        
        # Message tracking
        received_messages = []
        
        async def monitor_uniq(message: aio_pika.IncomingMessage):
            async with message.process():
                received_messages.append(message.body)
                logger.info(f"📨 Received message #{len(received_messages)}: {len(message.body)} bytes")
                if len(message.body) >= 2:
                    logger.info(f"   Starts with: 0x{message.body[:2].hex()}")
                    if message.body[:2] in [b'\x01\x00', b'\x01\x01', b'\x01\x02']:
                        logger.info("   ✅ Valid Lightning Network gossip message!")
        
        await monitor_queue.consume(monitor_uniq)
        
        # Create and start dedup service
        logger.info("Starting dedup service...")
        config = Config()
        dedup_service = DedupService(config)
        
        # Start dedup service in the background
        dedup_task = asyncio.create_task(dedup_service.run())
        await asyncio.sleep(2)  # Give it time to start
        
        # Create test messages
        logger.info("Publishing test messages...")
        
        # Test message 1: Protobuf-wrapped channel_announcement
        lightning_msg1 = b'\x01\x00' + b'A' * 136  # 138 bytes total
        protobuf_msg1 = b'\x0a' + bytes([len(lightning_msg1)]) + lightning_msg1
        
        # Test message 2: Same message (should be deduplicated)
        protobuf_msg2 = b'\x0a' + bytes([len(lightning_msg1)]) + lightning_msg1
        
        # Test message 3: Different message (node_announcement)
        lightning_msg3 = b'\x01\x01' + b'B' * 136  # 138 bytes total  
        protobuf_msg3 = b'\x0a' + bytes([len(lightning_msg3)]) + lightning_msg3
        
        # Publish messages
        logger.info(f"Publishing message 1: {len(protobuf_msg1)} bytes")
        await raw_exchange.publish(Message(protobuf_msg1), routing_key="")
        await asyncio.sleep(0.5)
        
        logger.info(f"Publishing message 2 (duplicate): {len(protobuf_msg2)} bytes")  
        await raw_exchange.publish(Message(protobuf_msg2), routing_key="")
        await asyncio.sleep(0.5)
        
        logger.info(f"Publishing message 3 (different): {len(protobuf_msg3)} bytes")
        await raw_exchange.publish(Message(protobuf_msg3), routing_key="")
        await asyncio.sleep(2)
        
        # Wait for processing
        logger.info("Waiting for message processing...")
        await asyncio.sleep(3)
        
        # Check results
        logger.info(f"\n=== Results ===")
        logger.info(f"Published: 3 messages (2 unique)")
        logger.info(f"Received: {len(received_messages)} messages")
        
        if len(received_messages) == 2:
            logger.info("✅ Deduplication working correctly!")
            logger.info("   - Duplicate message was filtered out")
            logger.info("   - 2 unique messages were forwarded")
        elif len(received_messages) == 3:
            logger.info("⚠️  Deduplication may not be working - all 3 messages forwarded")
        elif len(received_messages) == 0:
            logger.info("❌ No messages received - dedup service may not be processing")
        else:
            logger.info(f"🤔 Unexpected result: {len(received_messages)} messages received")
        
        # Check database
        import sqlite3
        conn = sqlite3.connect(config.database_path)
        db_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        conn.close()
        logger.info(f"Database contains: {db_count} unique messages")
        
        # Cleanup
        logger.info("Stopping dedup service...")
        dedup_task.cancel()
        try:
            await dedup_task
        except asyncio.CancelledError:
            pass
        
        await connection.close()
        logger.info("✅ Test completed")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_full_flow())