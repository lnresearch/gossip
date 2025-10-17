#!/usr/bin/env python3
"""
Test script to investigate local RabbitMQ server and test dedup service.
"""

import asyncio
import logging
import aio_pika
from aio_pika import ExchangeType, Message

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LOCAL_URL = "amqp://guest:guest@localhost:35672/"


async def test_local_rabbitmq():
    """Test local RabbitMQ connection and setup."""
    try:
        logger.info(f"Connecting to local RabbitMQ: {LOCAL_URL}")
        connection = await aio_pika.connect_robust(LOCAL_URL)
        channel = await connection.channel()
        
        logger.info("✅ Successfully connected to local RabbitMQ!")
        
        # Declare the exchanges that should exist for our pipeline
        logger.info("\n=== Setting up exchanges ===")
        
        # Raw exchange (where glbridge publishes)
        raw_exchange = await channel.declare_exchange(
            "lnr.gossip.raw",
            ExchangeType.DIRECT,
            durable=True
        )
        logger.info("✅ Declared lnr.gossip.raw exchange")
        
        # Unique exchange (where dedup publishes)
        uniq_exchange = await channel.declare_exchange(
            "lnr.gossip.uniq",
            ExchangeType.FANOUT,
            durable=True
        )
        logger.info("✅ Declared lnr.gossip.uniq exchange")
        
        # Create a test queue bound to uniq exchange to monitor output
        test_queue = await channel.declare_queue(
            "test.uniq.monitor",
            exclusive=True,
            auto_delete=True
        )
        await test_queue.bind(uniq_exchange)
        logger.info("✅ Created test queue bound to uniq exchange")
        
        # Test publishing a protobuf-wrapped message to raw exchange
        logger.info("\n=== Publishing test message ===")
        
        # Create a test protobuf-wrapped Lightning Network message
        # This simulates what glbridge would publish from router.gossip
        # Format: 0x0a + varint_length + Lightning_message
        lightning_msg = b'\x01\x00' + b'\x00' * 136  # channel_announcement (138 bytes total)
        protobuf_length = len(lightning_msg)
        
        # Encode length as varint (simple case for small numbers)
        if protobuf_length < 128:
            varint_bytes = bytes([protobuf_length])
        else:
            # More complex varint encoding for larger numbers
            varint_bytes = b''
            temp = protobuf_length
            while temp >= 128:
                varint_bytes += bytes([temp & 0x7F | 0x80])
                temp >>= 7
            varint_bytes += bytes([temp])
        
        protobuf_msg = b'\x0a' + varint_bytes + lightning_msg
        
        logger.info(f"Publishing test message: {len(protobuf_msg)} bytes (protobuf-wrapped)")
        logger.info(f"   Protobuf header: 0x{protobuf_msg[:4].hex()}")
        logger.info(f"   Inner Lightning message: 0x{lightning_msg[:2].hex()} ({len(lightning_msg)} bytes)")
        
        # Publish to raw exchange
        test_message = Message(protobuf_msg)
        await raw_exchange.publish(test_message, routing_key="")
        logger.info("✅ Published test message to lnr.gossip.raw")
        
        # Set up consumer to monitor uniq queue
        logger.info("\n=== Monitoring uniq queue ===")
        received_messages = 0
        
        async def monitor_uniq(message: aio_pika.IncomingMessage):
            nonlocal received_messages
            async with message.process():
                received_messages += 1
                logger.info(f"📨 Received unique message #{received_messages}: {len(message.body)} bytes")
                if len(message.body) >= 2:
                    logger.info(f"   Starts with: 0x{message.body[:2].hex()}")
                    # Check if it's a valid Lightning message
                    if message.body[:2] in [b'\x01\x00', b'\x01\x01', b'\x01\x02']:
                        logger.info("   ✅ Valid Lightning Network gossip message!")
        
        await test_queue.consume(monitor_uniq)
        
        logger.info("🔄 Waiting 5 seconds to see if dedup service processes the message...")
        await asyncio.sleep(5)
        
        if received_messages == 0:
            logger.info("⚠️  No messages received in uniq queue")
            logger.info("   This suggests the dedup service might not be running or processing messages")
        else:
            logger.info(f"✅ Received {received_messages} messages in uniq queue")
        
        await connection.close()
        logger.info("\n🔌 Disconnected from local RabbitMQ")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_local_rabbitmq())